// worker.c's apply dispatch + handlers and the execReplication.c machinery
// they ride on (RelationFindReplTupleByIndex/Seq, tuples_equal,
// ExecSimpleRelationInsert/Update/Delete renderings).
//
// Renderings vs C:
// - ExecSimpleRelation* are inlined here over the port's proven primitives:
//   exec_compute_stored_generated + exec_constraints (nodemodifytable),
//   simple_table_tuple_insert/update/delete (tableam), ExecOpenIndices/
//   ExecInsertIndexTuples (execindexing). Row TRIGGERS on the target refuse
//   loudly (trigger firing is not wired here).
// - build_replindex_scan_key uses the type-cache default btree equality
//   (TYPECACHE_EQ_OPR) instead of the index opclass's opfamily member;
//   identical for default opclasses, divergence recorded for custom ones.
// - The DirtySnapshot lookup snapshot is GetLatestSnapshot + C's
//   table_tuple_lock retry protocol.
use std::ffi::CString;
use std::rc::Rc;

use datum::Datum;
use elog::ereport;
use logicalproto::{
    LogicalRepTupleData, Reader, LOGICALREP_COLUMN_BINARY, LOGICALREP_COLUMN_TEXT,
    LOGICALREP_COLUMN_UNCHANGED,
};
use logicalrelation::LogicalRepRelMapEntry;
use mcx::Mcx;
use types_core::{InvalidOid, InvalidTransactionId, Oid, TransactionId};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_BINARY_REPRESENTATION,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_PROTOCOL_VIOLATION,
    ERRCODE_T_R_SERIALIZATION_FAILURE, ERRCODE_UNDEFINED_FUNCTION, ERROR, LOG,
};
use types_rel::Relation;
use types_scan::scankey::{ScanKeyData, SK_ISNULL, SK_SEARCHNULL};
use types_slot::SlotData;

use walreceiver::client::PgConn;

use crate::{
    loc, my_sub, reset_apply_error_context_info, set_apply_error_context_attnum,
    set_apply_error_context_command, set_apply_error_context_rel, set_apply_error_context_xact,
    IN_REMOTE_TRANSACTION, REMOTE_FINAL_LSN,
};

pub(crate) use backend_status_seams::BackendState;

// pgstat_report_activity(state, NULL) (backend_status.c): the apply worker
// reports RUNNING at the start of a remote transaction / streamed chunk and
// IDLE once it is finished, so pg_stat_activity.state tracks it as in C.
pub(crate) fn report_activity(state: BackendState) {
    backend_status_seams::pgstat_report_activity::call(state, None);
}

// Message-type bytes (logicalproto.h LogicalRepMsgType).
const MSG_BEGIN: u8 = b'B';
const MSG_COMMIT: u8 = b'C';
const MSG_ORIGIN: u8 = b'O';
const MSG_INSERT: u8 = b'I';
const MSG_UPDATE: u8 = b'U';
const MSG_DELETE: u8 = b'D';
const MSG_TRUNCATE: u8 = b'T';
const MSG_RELATION: u8 = b'R';
const MSG_TYPE: u8 = b'Y';
const MSG_MESSAGE: u8 = b'M';
const MSG_STREAM_START: u8 = b'S';
const MSG_STREAM_STOP: u8 = b'E';
const MSG_STREAM_COMMIT: u8 = b'c';
const MSG_STREAM_ABORT: u8 = b'A';
const MSG_BEGIN_PREPARE: u8 = b'b';
const MSG_PREPARE: u8 = b'P';
const MSG_COMMIT_PREPARED: u8 = b'K';
const MSG_ROLLBACK_PREPARED: u8 = b'r';
const MSG_STREAM_PREPARE: u8 = b'p';

pub(crate) fn logicalrep_relmap_prepare() -> PgResult<()> {
    logicalrelation::logicalrep_relmap_init()
}

// Per-handler input-function cache: one FmgrInfo per local column, alive
// until after the DML executes. THE LIFETIME MATTERS: this port's fmgr
// convention lets a function's result live in flinfo-owned scratch
// (fn_extra, e.g. fc_textin's OutBuf) — dropping the FmgrInfo frees the
// returned datum's storage. COPY keeps in_functions alive for the whole
// statement for the same reason; a per-call FmgrInfo here produced
// nondeterministic dangling varlenas in heap_form_tuple (round-4 crash).
pub(crate) struct InFuncs {
    per_col: Vec<Option<(fmgr::FmgrInfo, Oid)>>,
    // Receive functions for LOGICALREP_COLUMN_BINARY columns, cached under
    // the same lifetime discipline.
    recv_per_col: Vec<Option<(fmgr::FmgrInfo, Oid)>>,
}

impl InFuncs {
    fn new(natts: usize) -> Self {
        InFuncs {
            per_col: (0..natts).map(|_| None).collect(),
            recv_per_col: (0..natts).map(|_| None).collect(),
        }
    }
    fn get(&mut self, i: usize, atttypid: Oid) -> PgResult<&mut (fmgr::FmgrInfo, Oid)> {
        if self.per_col[i].is_none() {
            let (typinput, typioparam) = lsyscache::getTypeInputInfo(atttypid)?;
            self.per_col[i] = Some((fmgr_seams::fmgr_info::call(typinput)?, typioparam));
        }
        Ok(self.per_col[i].as_mut().expect("just filled"))
    }
    fn get_recv(&mut self, i: usize, atttypid: Oid) -> PgResult<&mut (fmgr::FmgrInfo, Oid)> {
        if self.recv_per_col[i].is_none() {
            let (typreceive, typioparam) = lsyscache::getTypeBinaryInputInfo(atttypid)?;
            self.recv_per_col[i] = Some((fmgr_seams::fmgr_info::call(typreceive)?, typioparam));
        }
        Ok(self.recv_per_col[i].as_mut().expect("just filled"))
    }
}

// The shared per-column conversion of slot_store_data / slot_modify_data
// (worker.c:826,941): TEXT through the type input function, BINARY through
// the type receive function (getTypeBinaryInputInfo + OidReceiveFunctionCall),
// anything else NULL.
fn slot_store_datum<'mcx>(
    mcx: Mcx<'mcx>,
    infuncs: &mut InFuncs,
    i: usize,
    atttypid: Oid,
    atttypmod: i32,
    colstatus: u8,
    bytes: &[u8],
    remoteattnum: usize,
) -> PgResult<(Datum, bool)> {
    match colstatus {
        LOGICALREP_COLUMN_TEXT => {
            // Publisher-supplied TEXT bytes are trusted only to the extent
            // that the remote honored the client_encoding we requested at
            // connect; a hostile/compromised publisher (or an honest
            // SQL_ASCII publisher) can ship bytes invalid in the subscriber's
            // database encoding. C tolerates the resulting mojibake because it
            // is byte-oriented, but this port has from_utf8_unchecked sinks
            // (e.g. jsonpath_exec over stored jsonb) that rely on the engine
            // invariant that stored strings are valid in the database
            // encoding; feeding them invalid bytes is UB. Verify against the
            // database encoding — as C's pg_client_to_server would when
            // client_encoding == database encoding — before the input
            // function runs. SQL_ASCII accepts every byte, so valid behavior
            // is preserved. This covers every apply path (direct, streamed,
            // parallel), which all funnel through here.
            mbutils::pg_verifymbstr(bytes, false)?;
            let cstr = CString::new(bytes).map_err(|_| {
                Box::new(types_error::PgError::error(
                    "invalid text column data in logical replication message".to_string(),
                ))
            })?;
            let (flinfo, typioparam) = infuncs.get(i, atttypid)?;
            let ioparam = *typioparam;
            let d = fmgr::soft::input_function_call(
                flinfo,
                Some(cstr.as_c_str()),
                ioparam,
                atttypmod,
                mcx,
            )?;
            Ok((d, false))
        }
        LOGICALREP_COLUMN_BINARY => {
            let (flinfo, typioparam) = infuncs.get_recv(i, atttypid)?;
            let ioparam = *typioparam;
            let d = receive_binary_column(mcx, flinfo, ioparam, atttypmod, bytes, remoteattnum)?;
            Ok((d, false))
        }
        // NULL from remote (an unexpected UNCHANGED is treated as NULL too).
        _ => Ok((Datum::null(), true)),
    }
}

// The BINARY decode itself: OidReceiveFunctionCall over the column bytes,
// erroring as C when the receive function doesn't eat the whole buffer.
pub(crate) fn receive_binary_column<'mcx>(
    mcx: Mcx<'mcx>,
    flinfo: &mut fmgr::FmgrInfo,
    typioparam: Oid,
    atttypmod: i32,
    bytes: &[u8],
    remoteattnum: usize,
) -> PgResult<Datum> {
    let mut buf = stringinfo::StringInfo::from_vec(mcx::slice_in(mcx, bytes)?)?;
    let d = fmgr::receive_function_call(flinfo, Some(&mut buf), typioparam, atttypmod, mcx)?;
    // Trouble if it didn't eat the whole buffer.
    if buf.cursor != buf.len() {
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_BINARY_REPRESENTATION)
            .errmsg(format!(
                "incorrect binary data format in logical replication column {}",
                remoteattnum + 1
            ))
            .finish(loc("slot_store_data"))?;
    }
    Ok(d)
}

// begin_replication_step (worker.c:501).
pub(crate) fn begin_replication_step(mcx: Mcx<'_>) -> PgResult<()> {
    // worker.c:503: every step is a new statement for statement_timestamp()
    // and the statement-start bookkeeping.
    xact::SetCurrentStatementStartTimestamp();

    if !xact::IsTransactionState() {
        xact::StartTransactionCommand()?;
        crate::maybe_reread_subscription(mcx)?;
    }
    let snap = snapmgr::GetTransactionSnapshot()?;
    snapmgr::PushActiveSnapshot(&snap)?;
    Ok(())
}

// end_replication_step (worker.c:524).
pub(crate) fn end_replication_step() -> PgResult<()> {
    snapmgr::PopActiveSnapshot()?;
    xact::CommandCounterIncrement()
}

// should_apply_changes_for_rel (worker.c:461). Non-READY
// states belong to tablesync (inc E) and refuse loudly rather than desync.
fn should_apply_changes_for_rel(entry: &LogicalRepRelMapEntry) -> PgResult<bool> {
    const SUBREL_STATE_READY: u8 = b'r';
    const SUBREL_STATE_SYNCDONE: u8 = b's';
    const SUBREL_STATE_UNKNOWN: u8 = 0;
    if crate::tablesync::AM_TABLESYNC_WORKER.with(std::cell::Cell::get) {
        // The tablesync worker applies only its own table's changes
        // (worker.c:461); localreloid match is implied by the relmap entry.
        let myrelid = launcher::worker_snapshot(launcher::my_worker_slot().expect("attached"))
            .map(|w| w.relid)
            .unwrap_or(types_core::InvalidOid);
        return Ok(entry.localreloid == myrelid);
    }
    if crate::parallel::am_parallel_apply_worker() {
        // We can't decide for a non-READY table (remote_final_lsn unknown).
        if entry.state != SUBREL_STATE_READY && entry.state != SUBREL_STATE_UNKNOWN {
            let name = my_sub(|s| s.name.clone());
            ereport(ERROR)
                .errcode(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "logical replication parallel apply worker for subscription \"{name}\" will stop"
                ))
                .errdetail("Cannot handle streamed replication transactions using parallel apply workers until all tables have been synchronized.")
                .finish(loc("should_apply_changes_for_rel"))?;
        }
        return Ok(entry.state == SUBREL_STATE_READY);
    }
    Ok(match entry.state {
        SUBREL_STATE_READY => true,
        SUBREL_STATE_SYNCDONE => entry.statelsn <= REMOTE_FINAL_LSN.get(),
        // INIT/DATASYNC/FINISHEDCOPY/SYNCWAIT/CATCHUP: the tablesync worker
        // owns those changes; the leader skips them.
        _ => false,
    })
}

// apply_dispatch (worker.c:3368). C intercepts streamed chunks inside each
// data-message handler (handle_streamed_transaction at their entry); doing it
// once here is equivalent — inside a streamed chunk every data message spools
// to the transaction's file instead of applying.
pub(crate) fn apply_dispatch(
    mcx: Mcx<'static>,
    conn: Option<&mut PgConn>,
    buf: &[u8],
) -> PgResult<()> {
    // pq_getmsgbyte (worker.c:3385): an empty payload is 08P01.
    let action = Reader::new(buf).get_byte()?;
    // Set the current command being applied; this is re-entered when
    // applying spooled changes, so the current command is saved
    // (worker.c:3393). C restores it only on the normal exit
    // (worker.c:3486) — an error leaves the innermost command for the
    // callback to report.
    let saved_command = set_apply_error_context_command(action);
    let r = apply_dispatch_guts(mcx, conn, action, buf);
    if r.is_ok() {
        set_apply_error_context_command(saved_command);
    }
    r
}

fn apply_dispatch_guts(
    mcx: Mcx<'static>,
    mut conn: Option<&mut PgConn>,
    action: u8,
    buf: &[u8],
) -> PgResult<()> {
    // C checks is_skipping_changes() at the entry of the four data-
    // modification handlers only (worker.c:2404,2564,2769,3257); RELATION/
    // TYPE/MESSAGE are processed even while skipping.
    if matches!(action, MSG_INSERT | MSG_UPDATE | MSG_DELETE | MSG_TRUNCATE)
        && crate::is_skipping_changes()
    {
        return Ok(());
    }
    // In a streamed chunk (or in a parallel apply worker) every data message
    // carries the (sub)txn xid first; the payload starts after it.
    let mut payload = &buf[1..];
    if matches!(
        action,
        MSG_RELATION | MSG_TYPE | MSG_INSERT | MSG_UPDATE | MSG_DELETE | MSG_TRUNCATE | MSG_MESSAGE
    ) {
        use crate::stream_apply::StreamedHandling;
        match crate::stream_apply::handle_streamed_transaction(mcx, action, buf)? {
            StreamedHandling::Consumed => return Ok(()),
            StreamedHandling::ContinueAt(off) => payload = &buf[off..],
            StreamedHandling::NotStreamed => {}
        }
    }
    let mut r = Reader::new(payload);
    match action {
        MSG_BEGIN => apply_handle_begin(&mut r),
        MSG_COMMIT => apply_handle_commit(mcx, conn, &mut r),
        MSG_ORIGIN => apply_handle_origin(),
        MSG_RELATION => apply_handle_relation(mcx, &mut r),
        MSG_TYPE => {
            let _ = logicalproto::logicalrep_read_typ(&mut r)?;
            Ok(())
        }
        MSG_INSERT => apply_handle_insert(mcx, &mut r),
        MSG_UPDATE => apply_handle_update(mcx, &mut r),
        MSG_DELETE => apply_handle_delete(mcx, &mut r),
        MSG_TRUNCATE => apply_handle_truncate(mcx, &mut r),
        MSG_MESSAGE => Ok(()), // transactional messages are ignored by apply
        MSG_STREAM_START => crate::stream_apply::apply_handle_stream_start(mcx, buf),
        MSG_STREAM_STOP => crate::stream_apply::apply_handle_stream_stop(mcx, buf),
        MSG_STREAM_COMMIT => {
            crate::stream_apply::apply_handle_stream_commit(mcx, conn.as_deref_mut(), buf)
        }
        MSG_STREAM_ABORT => crate::stream_apply::apply_handle_stream_abort(mcx, buf),
        MSG_STREAM_PREPARE => {
            crate::stream_apply::apply_handle_stream_prepare(mcx, conn.as_deref_mut(), buf)
        }
        MSG_BEGIN_PREPARE => apply_handle_begin_prepare(&mut r),
        MSG_PREPARE => apply_handle_prepare(mcx, conn, &mut r),
        MSG_COMMIT_PREPARED => apply_handle_commit_prepared(mcx, conn, &mut r),
        MSG_ROLLBACK_PREPARED => apply_handle_rollback_prepared(mcx, conn, &mut r),
        other => {
            ereport(ERROR)
                .errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(format!("invalid logical replication message type \"??? ({other})\""))
                .finish(loc("apply_dispatch"))?;
            unreachable!();
        }
    }
}

// apply_handle_origin (worker.c:1435): the ORIGIN message can only come
// inside a streamed transaction, or inside a remote transaction before any
// actual writes; its contents are not used.
fn apply_handle_origin() -> PgResult<()> {
    if !crate::stream_apply::in_streamed_transaction()
        && (!IN_REMOTE_TRANSACTION.get()
            || (xact::IsTransactionState()
                && !crate::tablesync::AM_TABLESYNC_WORKER.with(std::cell::Cell::get)))
    {
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("ORIGIN message sent out of order")
            .finish(loc("apply_handle_origin"))?;
        unreachable!();
    }
    Ok(())
}

// apply_handle_begin (worker.c:985).
fn apply_handle_begin(r: &mut Reader<'_>) -> PgResult<()> {
    let begin = logicalproto::logicalrep_read_begin(r)?;
    set_apply_error_context_xact(begin.xid, begin.final_lsn);

    REMOTE_FINAL_LSN.set(begin.final_lsn);
    crate::maybe_start_skipping_changes(begin.final_lsn);
    IN_REMOTE_TRANSACTION.set(true);

    report_activity(BackendState::STATE_RUNNING);
    Ok(())
}

// apply_handle_commit (worker.c:1010).
fn apply_handle_commit(
    mcx: Mcx<'static>,
    conn: Option<&mut PgConn>,
    r: &mut Reader<'_>,
) -> PgResult<()> {
    let commit = logicalproto::logicalrep_read_commit(r)?;

    if commit.commit_lsn != REMOTE_FINAL_LSN.get() {
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "incorrect commit LSN {:X}/{:X} in commit message (expected {:X}/{:X})",
                (commit.commit_lsn >> 32) as u32,
                commit.commit_lsn as u32,
                (REMOTE_FINAL_LSN.get() >> 32) as u32,
                REMOTE_FINAL_LSN.get() as u32,
            ))
            .finish(loc("apply_handle_commit"))?;
    }

    apply_handle_commit_internal(mcx, &commit)?;
    crate::tablesync::process_syncing_tables(mcx, conn, commit.end_lsn)?;

    report_activity(BackendState::STATE_IDLE);
    reset_apply_error_context_info();
    Ok(())
}

// apply_handle_commit_internal (worker.c:2258), shared by the live COMMIT and
// the streamed STREAM COMMIT replay.
pub(crate) fn apply_handle_commit_internal(
    mcx: Mcx<'static>,
    commit: &logicalproto::LogicalRepCommitData,
) -> PgResult<()> {
    if crate::is_skipping_changes() {
        crate::stop_skipping_changes();

        // Start a new transaction to clear the subskiplsn, if not started
        // yet.
        if !xact::IsTransactionState() {
            xact::StartTransactionCommand()?;
        }
    }

    if xact::IsTransactionState() {
        // The transaction is either non-empty or skipped, so we clear the
        // subskiplsn.
        crate::clear_subscription_skip_lsn(mcx, commit.commit_lsn)?;

        // Update origin state so streaming restarts from the right position
        // after a crash: the session origin lsn/timestamp ride the local
        // commit record (origin crate seams feed xact's commit writer).
        origin::set_replorigin_session_origin_lsn(commit.end_lsn);
        origin::set_replorigin_session_origin_timestamp(commit.committime);

        xact::CommitTransactionCommand()?;

        if xact::IsTransactionBlock() {
            xact::EndTransactionBlock(false)?;
            xact::CommitTransactionCommand()?;
        }

        // worker.c:2305: flush pending stats (conflict counters among them)
        // now that no transaction is open.
        pgstat::pending::pgstat_report_stat(false);

        let local_end = transam_xlog_seams::xact_last_commit_end::call();
        crate::store_flush_position(commit.end_lsn, local_end);
    } else {
        // Empty transaction: process pending invals.
        inval::local::AcceptInvalidationMessages()?;
        crate::maybe_reread_subscription(mcx)?;
    }

    IN_REMOTE_TRANSACTION.set(false);
    Ok(())
}

// apply_handle_begin_prepare (worker.c:1036).
fn apply_handle_begin_prepare(r: &mut Reader<'_>) -> PgResult<()> {
    // Tablesync should never receive prepare.
    if crate::tablesync::AM_TABLESYNC_WORKER.with(std::cell::Cell::get) {
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("tablesync worker received a BEGIN PREPARE message")
            .finish(loc("apply_handle_begin_prepare"))?;
    }

    let begin = logicalproto::logicalrep_read_begin_prepare(r)?;
    set_apply_error_context_xact(begin.xid, begin.prepare_lsn);

    REMOTE_FINAL_LSN.set(begin.prepare_lsn);
    crate::maybe_start_skipping_changes(begin.prepare_lsn);
    IN_REMOTE_TRANSACTION.set(true);

    report_activity(BackendState::STATE_RUNNING);
    Ok(())
}

// apply_handle_prepare_internal (worker.c:1065), shared by the live PREPARE
// and the streamed STREAM PREPARE replay.
pub(crate) fn apply_handle_prepare_internal(
    prepare_data: &logicalproto::LogicalRepPreparedTxnData,
) -> PgResult<()> {
    // Compute a unique GID for the two_phase transaction; the publisher's own
    // GID could deadlock with multiple subscriptions from the same node (see
    // comments atop worker.c).
    let gid = twophase::TwoPhaseTransactionGid(my_sub(|s| s.oid), prepare_data.xid)?;

    // BeginTransactionBlock is necessary to balance the EndTransactionBlock
    // called within the PrepareTransactionBlock below.
    if !xact::IsTransactionBlock() {
        xact::BeginTransactionBlock()?;
        xact::CommitTransactionCommand()?; // Completes the preceding Begin command.
    }

    // Update origin state so we can restart streaming from the correct
    // position in case of a crash.
    origin::set_replorigin_session_origin_lsn(prepare_data.end_lsn);
    origin::set_replorigin_session_origin_timestamp(prepare_data.prepare_time);

    xact::PrepareTransactionBlock(&gid)?;
    Ok(())
}

// apply_handle_prepare (worker.c:1102).
fn apply_handle_prepare(
    mcx: Mcx<'static>,
    conn: Option<&mut PgConn>,
    r: &mut Reader<'_>,
) -> PgResult<()> {
    let prepare_data = logicalproto::logicalrep_read_prepare(r)?;

    if prepare_data.prepare_lsn != REMOTE_FINAL_LSN.get() {
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "incorrect prepare LSN {:X}/{:X} in prepare message (expected {:X}/{:X})",
                (prepare_data.prepare_lsn >> 32) as u32,
                prepare_data.prepare_lsn as u32,
                (REMOTE_FINAL_LSN.get() >> 32) as u32,
                REMOTE_FINAL_LSN.get() as u32,
            ))
            .finish(loc("apply_handle_prepare"))?;
    }

    // Unlike commit, we always prepare the transaction even if nothing
    // changed in it: at commit prepared time we won't know whether we skipped
    // preparing.
    begin_replication_step(mcx)?;
    apply_handle_prepare_internal(&prepare_data)?;
    end_replication_step()?;
    xact::CommitTransactionCommand()?;
    // worker.c:1141.
    pgstat::pending::pgstat_report_stat(false);

    // The prepare record is always flushed, so acknowledging the remote_end
    // LSN with an invalid local LSN is fine (worker.c:1131).
    crate::store_flush_position(prepare_data.end_lsn, types_core::InvalidXLogRecPtr);

    IN_REMOTE_TRANSACTION.set(false);

    // Process any tables that are being synchronized in parallel.
    crate::tablesync::process_syncing_tables(mcx, conn, prepare_data.end_lsn)?;

    // Since the transaction is already prepared, a crash before clearing the
    // subskiplsn leaves it set, but the transaction won't be resent; the
    // subskiplsn is then cleared when finishing the next transaction.
    crate::stop_skipping_changes();
    crate::clear_subscription_skip_lsn(mcx, prepare_data.prepare_lsn)?;

    report_activity(BackendState::STATE_IDLE);
    reset_apply_error_context_info();
    Ok(())
}

// apply_handle_commit_prepared (worker.c:1173).
fn apply_handle_commit_prepared(
    mcx: Mcx<'static>,
    conn: Option<&mut PgConn>,
    r: &mut Reader<'_>,
) -> PgResult<()> {
    let prepare_data = logicalproto::logicalrep_read_commit_prepared(r)?;
    set_apply_error_context_xact(prepare_data.xid, prepare_data.commit_lsn);

    let gid = twophase::TwoPhaseTransactionGid(my_sub(|s| s.oid), prepare_data.xid)?;

    // There is no transaction when COMMIT PREPARED is called.
    begin_replication_step(mcx)?;

    // Update origin state so we can restart streaming from the correct
    // position in case of a crash.
    origin::set_replorigin_session_origin_lsn(prepare_data.end_lsn);
    origin::set_replorigin_session_origin_timestamp(prepare_data.commit_time);

    twophase::FinishPreparedTransaction(&gid, true)?;
    end_replication_step()?;
    xact::CommitTransactionCommand()?;
    // worker.c:1208.
    pgstat::pending::pgstat_report_stat(false);

    let local_end = transam_xlog_seams::xact_last_commit_end::call();
    crate::store_flush_position(prepare_data.end_lsn, local_end);
    IN_REMOTE_TRANSACTION.set(false);

    crate::tablesync::process_syncing_tables(mcx, conn, prepare_data.end_lsn)?;

    crate::clear_subscription_skip_lsn(mcx, prepare_data.end_lsn)?;

    report_activity(BackendState::STATE_IDLE);
    reset_apply_error_context_info();
    Ok(())
}

// apply_handle_rollback_prepared (worker.c:1222).
fn apply_handle_rollback_prepared(
    mcx: Mcx<'static>,
    conn: Option<&mut PgConn>,
    r: &mut Reader<'_>,
) -> PgResult<()> {
    let rollback_data = logicalproto::logicalrep_read_rollback_prepared(r)?;
    set_apply_error_context_xact(rollback_data.xid, rollback_data.rollback_end_lsn);

    let gid = twophase::TwoPhaseTransactionGid(my_sub(|s| s.oid), rollback_data.xid)?;

    // It is possible that we haven't received the prepare because it occurred
    // before the walsender reached a consistent point or two_phase was not
    // yet enabled; skip the rollback in that case.
    if twophase::LookupGXact(&gid, rollback_data.prepare_end_lsn, rollback_data.prepare_time)? {
        // Update origin state so we can restart streaming from the correct
        // position in case of a crash.
        origin::set_replorigin_session_origin_lsn(rollback_data.rollback_end_lsn);
        origin::set_replorigin_session_origin_timestamp(rollback_data.rollback_time);

        // There is no transaction when ROLLBACK PREPARED is called.
        begin_replication_step(mcx)?;
        twophase::FinishPreparedTransaction(&gid, false)?;
        end_replication_step()?;
        xact::CommitTransactionCommand()?;
        pgstat::pending::pgstat_report_stat(false);

        crate::clear_subscription_skip_lsn(mcx, rollback_data.rollback_end_lsn)?;
    }

    // worker.c:1269.
    pgstat::pending::pgstat_report_stat(false);

    // The rollback WAL record is always flushed (worker.c:1259).
    crate::store_flush_position(
        rollback_data.rollback_end_lsn,
        types_core::InvalidXLogRecPtr,
    );
    IN_REMOTE_TRANSACTION.set(false);

    crate::tablesync::process_syncing_tables(mcx, conn, rollback_data.rollback_end_lsn)?;

    report_activity(BackendState::STATE_IDLE);
    reset_apply_error_context_info();
    Ok(())
}

// apply_handle_relation (worker.c:2318).
fn apply_handle_relation(mcx: Mcx<'_>, r: &mut Reader<'_>) -> PgResult<()> {
    // No transaction in C (worker.c:2317): the relmap update touches only the
    // in-memory map. Opening one here used to leave it dangling when RELATION
    // is applied OUTSIDE a remote transaction (the parallel-streaming leader
    // applies forwarded RELATION/TYPE messages between chunks).
    let _ = mcx;
    let rel = logicalproto::logicalrep_read_rel(r)?;
    logicalrelation::logicalrep_relmap_update(&rel);
    Ok(())
}

// slot_store_data / slot_modify_data (worker.c:811, :928): a remote column
// the received tuple does not carry is a protocol violation.
pub(crate) fn tuple_column_check(remoteattnum: usize, ncols: usize) -> PgResult<()> {
    if remoteattnum >= ncols {
        ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "logical replication column {} not found in tuple: only {} column(s) received",
                remoteattnum + 1,
                ncols
            ))
            .finish(loc("slot_store_data"))?;
        unreachable!();
    }
    Ok(())
}

// slot_store_data (worker.c:791).
fn slot_store_data<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
    entry: &LogicalRepRelMapEntry,
    rel: &Relation<'mcx>,
    tup: &LogicalRepTupleData,
    infuncs: &mut InFuncs,
) -> PgResult<()> {
    let natts = rel.rd_att.natts as usize;
    exectuples::exec_clear_tuple(slot, mcx);

    for i in 0..natts {
        let att = rel.rd_att.attr(i);
        let remote = entry.attrmap.get(i).copied().unwrap_or(-1);
        let (value, isnull) = if !att.attisdropped && remote >= 0 {
            let m = remote as usize;
            tuple_column_check(m, tup.ncols)?;
            let bytes = tup.colvalues[m].as_deref().unwrap_or(&[]);
            // Set attnum for error callback (worker.c:819); reset after the
            // conversion (worker.c:868).
            set_apply_error_context_attnum(m as i32);
            let v = slot_store_datum(
                mcx,
                infuncs,
                i,
                att.atttypid,
                att.atttypmod,
                tup.colstatus[m],
                bytes,
                m,
            )?;
            set_apply_error_context_attnum(-1);
            v
        } else {
            (Datum::null(), true)
        };
        let base = slot.base_mut();
        base.tts_values[i] = value;
        base.tts_isnull[i] = isnull;
    }

    exectuples::exec_store_virtual_tuple(slot);
    Ok(())
}

// slot_fill_defaults (worker.c:734): evaluate non-NULL column defaults for
// subscriber-only columns on INSERT apply (tables with more columns on the
// subscriber than on the publisher); slot_store_data left them NULL.
// expression_planner is rendered as eval_const_expressions + fix_opfuncids
// (the COPY FROM defaults convention). Returns the compiled expressions as a
// keep-alive: a result datum may live in flinfo-owned scratch (see InFuncs),
// so the states must outlive the DML that consumes the slot.
// slot_fill_defaults' column filter (worker.c:757-766): dropped and generated
// columns never get defaults; replicated columns (attrmap >= 0) keep the
// received value.
pub(crate) fn needs_default_fill(attisdropped: bool, attgenerated: i8, remote: i16) -> bool {
    !attisdropped && attgenerated == 0 && remote < 0
}

fn slot_fill_defaults<'mcx>(
    mcx: Mcx<'mcx>,
    entry: &LogicalRepRelMapEntry,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<Vec<mcx::PgBox<'mcx, execexpr::ExprState<'mcx>>>> {
    let mut keep_alive = Vec::new();
    let natts = rel.rd_att.natts as usize;

    // We got all the data via replication: no need to evaluate anything.
    if natts == entry.remoterel.natts {
        return Ok(keep_alive);
    }

    let mut defmap = Vec::new();
    for i in 0..natts {
        let att = rel.rd_att.attr(i);
        let remote = entry.attrmap.get(i).copied().unwrap_or(-1);
        if !needs_default_fill(att.attisdropped, att.attgenerated, remote) {
            continue;
        }
        let Some(defexpr) = rewrite_handler::build_column_default(mcx, rel, i + 1)? else {
            continue;
        };
        let defexpr = clauses::eval_const_expressions(mcx, defexpr)?;
        nodes_core::fix_opfuncids(defexpr)?;
        let mut state = execexpr::exec_init_expr(mcx, Some(defexpr), execexpr::ParamBind::NONE)?
            .expect("column default expression");
        state.arm_result_mcx(mcx);
        keep_alive.push(state);
        defmap.push(i);
    }

    for (state, &i) in keep_alive.iter_mut().zip(defmap.iter()) {
        let mut slots = execexpr::EvalSlots { scan: None, inner: None, outer: None };
        let r = execexpr::exec_eval_expr(state, &mut slots)?;
        let base = slot.base_mut();
        base.tts_values[i] = r.value;
        base.tts_isnull[i] = r.isnull;
    }
    Ok(keep_alive)
}

// slot_modify_data (worker.c:892): copy srcslot, replace replicated columns.
fn slot_modify_data<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
    srcslot: &mut SlotData<'mcx>,
    entry: &LogicalRepRelMapEntry,
    rel: &Relation<'mcx>,
    tup: &LogicalRepTupleData,
    infuncs: &mut InFuncs,
) -> PgResult<()> {
    let natts = rel.rd_att.natts as usize;
    exectuples::exec_clear_tuple(slot, mcx);

    exectuples::slot_getallattrs(srcslot);
    for i in 0..natts {
        let (v, n) = {
            let sb = srcslot.base();
            (sb.tts_values[i], sb.tts_isnull[i])
        };
        let base = slot.base_mut();
        base.tts_values[i] = v;
        base.tts_isnull[i] = n;
    }

    for i in 0..natts {
        let att = rel.rd_att.attr(i);
        let remote = entry.attrmap.get(i).copied().unwrap_or(-1);
        if remote < 0 {
            continue;
        }
        let m = remote as usize;
        tuple_column_check(m, tup.ncols)?;
        if tup.colstatus[m] == LOGICALREP_COLUMN_UNCHANGED {
            continue;
        }
        let bytes = tup.colvalues[m].as_deref().unwrap_or(&[]);
        // Set attnum for error callback (worker.c:938); reset after the
        // conversion (worker.c:983).
        set_apply_error_context_attnum(m as i32);
        let (value, isnull) = slot_store_datum(
            mcx,
            infuncs,
            i,
            att.atttypid,
            att.atttypmod,
            tup.colstatus[m],
            bytes,
            m,
        )?;
        set_apply_error_context_attnum(-1);
        let base = slot.base_mut();
        base.tts_values[i] = value;
        base.tts_isnull[i] = isnull;
    }

    exectuples::exec_store_virtual_tuple(slot);
    Ok(())
}

// GetTupleTransactionInfo (conflict.c:62): the local tuple's xmin plus the
// commit timestamp data (timestamp, origin) of the transaction that created
// that version; the data half is None when track_commit_timestamp is off or
// the xid is outside the retained commit-ts range, as C's false return.
pub(crate) fn get_tuple_transaction_info(
    localslot: &SlotData<'_>,
) -> PgResult<(types_core::TransactionId, Option<(types_core::TimestampTz, types_core::RepOriginId)>)> {
    let mut isnull = false;
    let xmin_d = exectuples::slot_getsysattr(
        localslot,
        types_tuple::MinTransactionIdAttributeNumber,
        &mut isnull,
    )?;
    debug_assert!(!isnull);
    let xmin = xmin_d.as_u32();

    // The commit timestamp data is not available if track_commit_timestamp
    // is disabled.
    if !guc_tables::vars::track_commit_timestamp.read() {
        return Ok((xmin, None));
    }
    Ok((xmin, commit_ts::TransactionIdGetCommitTsData(xmin)?))
}

// The origin-differs LOG report (worker.c:2708-2725 / :2889-2899 /
// :3109-3127): ReportApplyConflict for CT_UPDATE_ORIGIN_DIFFERS (the remote
// new tuple stored into a fresh slot for the DETAIL) or
// CT_DELETE_ORIGIN_DIFFERS. `remoteslot` is the search tuple, `localslot`
// the local row it found.
#[allow(clippy::too_many_arguments)]
fn report_origin_differs<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    entry: &LogicalRepRelMapEntry,
    remoteslot: &mut SlotData<'mcx>,
    localslot: &mut SlotData<'mcx>,
    newtup: Option<&LogicalRepTupleData>,
    localxmin: types_core::TransactionId,
    localorigin: types_core::RepOriginId,
    localts: types_core::TimestampTz,
) -> PgResult<()> {
    use crate::conflict::{report_apply_conflict, ConflictTupleInfo, ConflictType};
    // The new tuple's datums live in this cache's scratch until the report
    // is done (see slot_store_data's InFuncs discipline).
    let mut infuncs = InFuncs::new(rel.rd_att.natts as usize);
    let mut newslot = match newtup {
        Some(newtup) => {
            let mut slot = tableam_real::table_slot_create(mcx, rel)?;
            slot_store_data(mcx, &mut slot, entry, rel, newtup, &mut infuncs)?;
            Some(slot)
        }
        None => None,
    };
    let ty = if newtup.is_some() {
        ConflictType::UpdateOriginDiffers
    } else {
        ConflictType::DeleteOriginDiffers
    };
    let mut conflicttuple = [ConflictTupleInfo {
        slot: Some(localslot),
        indexoid: InvalidOid,
        xmin: localxmin,
        origin: localorigin,
        ts: localts,
    }];
    report_apply_conflict(mcx, rel, LOG, ty, Some(remoteslot), newslot.as_mut(), &mut conflicttuple)
}

// The missing-row LOG report (worker.c:2746-2754 / :2911-2915 / :3083-3089):
// ReportApplyConflict for CT_UPDATE_MISSING (the remote new tuple stored into
// the unused local slot, as C reuses it) or CT_DELETE_MISSING.
fn report_row_missing<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    entry: &LogicalRepRelMapEntry,
    remoteslot: &mut SlotData<'mcx>,
    localslot: &mut SlotData<'mcx>,
    newtup: Option<&LogicalRepTupleData>,
) -> PgResult<()> {
    use crate::conflict::{report_apply_conflict, ConflictTupleInfo, ConflictType};
    let mut infuncs = InFuncs::new(rel.rd_att.natts as usize);
    let mut conflicttuple = [ConflictTupleInfo::missing()];
    match newtup {
        Some(newtup) => {
            slot_store_data(mcx, localslot, entry, rel, newtup, &mut infuncs)?;
            report_apply_conflict(
                mcx,
                rel,
                LOG,
                ConflictType::UpdateMissing,
                Some(remoteslot),
                Some(localslot),
                &mut conflicttuple,
            )
        }
        None => report_apply_conflict(
            mcx,
            rel,
            LOG,
            ConflictType::DeleteMissing,
            Some(remoteslot),
            None,
            &mut conflicttuple,
        ),
    }
}

// The run_as_owner arm shared by the DML handlers (worker.c:2427 etc.) and
// tablesync (tablesync.c:1515): unless the subscription opted out, any
// user-supplied code runs as the table owner. Returns the context to restore
// (None when running as owner was requested).
pub(crate) fn maybe_switch_to_table_owner(
    mcx: Mcx<'_>,
    relowner: Oid,
) -> PgResult<Option<types_core::UserContext>> {
    if my_sub(|s| s.runasowner) {
        return Ok(None);
    }
    let mut ucxt = types_core::UserContext::new(InvalidOid, 0, 0);
    init_small::SwitchToUntrustedUser(mcx, relowner, &mut ucxt)?;
    Ok(Some(ucxt))
}

pub(crate) fn restore_user_context(ucxt: &Option<types_core::UserContext>) -> PgResult<()> {
    if let Some(ucxt) = ucxt {
        init_small::RestoreUserContext(ucxt)?;
    }
    Ok(())
}

// The per-DML row-trigger state of C's ResultRelInfo (ri_TrigDesc,
// ri_TrigFunctions, ri_TrigWhenExprs). Fetched per apply operation; None when
// the target carries no triggers.
struct ApplyTrig<'mcx> {
    td: std::rc::Rc<types_trigger::TriggerDesc<'static>>,
    fmgr: trigger::TriggerFmgrCache,
    when: trigger::TriggerWhenCache<'mcx>,
}

fn apply_trig<'mcx>(rel: &Relation<'_>) -> PgResult<Option<ApplyTrig<'mcx>>> {
    if !rel.rd_hastriggers {
        return Ok(None);
    }
    Ok(relcache::RelationGetTriggerDesc(rel.rd_id)?.map(|td| ApplyTrig {
        td,
        fmgr: trigger::TriggerFmgrCache::default(),
        when: trigger::TriggerWhenCache::default(),
    }))
}

// check_relation_updatable (worker.c:2514).
fn check_relation_updatable(
    mcx: Mcx<'_>,
    rel: &Relation<'_>,
    entry: &LogicalRepRelMapEntry,
) -> PgResult<()> {
    // For partitioned tables, only the target partition's updatability
    // matters (aka has PK or RI defined for it) (worker.c:2520).
    if rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
        return Ok(());
    }
    if entry.updatable {
        return Ok(());
    }
    // Error mode, so being somewhat slow is fine: give the user the precise
    // reason (worker.c:2532).
    if logicalrelation::get_relation_identity_or_pk(mcx, rel)? != InvalidOid {
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "publisher did not send replica identity column expected by the logical \
                 replication target relation \"{}.{}\"",
                entry.remoterel.nspname, entry.remoterel.relname
            ))
            .finish(loc("check_relation_updatable"))?;
        unreachable!();
    }
    ereport(ERROR)
        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .errmsg(format!(
            "logical replication target relation \"{}.{}\" has neither REPLICA IDENTITY index \
             nor PRIMARY KEY and published relation does not have REPLICA IDENTITY FULL",
            entry.remoterel.nspname, entry.remoterel.relname
        ))
        .finish(loc("check_relation_updatable"))?;
    unreachable!();
}

// tuples_equal (execReplication.c:282).
fn tuples_equal<'mcx>(
    mcx: Mcx<'mcx>,
    slot1: &mut SlotData<'mcx>,
    slot2: &mut SlotData<'mcx>,
    rel: &Relation<'mcx>,
) -> PgResult<bool> {
    exectuples::slot_getallattrs(slot1);
    exectuples::slot_getallattrs(slot2);

    let natts = rel.rd_att.natts as usize;
    for i in 0..natts {
        let att = rel.rd_att.attr(i);
        if att.attisdropped || att.attgenerated != 0 {
            continue;
        }
        let (n1, v1) = {
            let b = slot1.base();
            (b.tts_isnull[i], b.tts_values[i])
        };
        let (n2, v2) = {
            let b = slot2.base();
            (b.tts_isnull[i], b.tts_values[i])
        };
        if n1 != n2 {
            return Ok(false);
        }
        if n1 {
            continue;
        }
        let typentry =
            typcache::lookup_type_cache(att.atttypid, typcache::TYPECACHE_EQ_OPR_FINFO)?;
        let mut finfo = typentry.eq_opr_finfo();
        if finfo.fn_oid == InvalidOid {
            // execReplication.c:326: ERRCODE_UNDEFINED_FUNCTION with the
            // type's SQL name (format_type_be).
            ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_FUNCTION)
                .errmsg(format!(
                    "could not identify an equality operator for type {}",
                    format_type::format_type_be(att.atttypid)?
                ))
                .finish(loc("tuples_equal"))?;
            unreachable!();
        }
        let eq = fmgr::fcinfo::function_call2_coll_in(&mut finfo, att.attcollation, mcx, v1, v2)?;
        if !eq.as_bool() {
            return Ok(false);
        }
    }
    Ok(true)
}

// build_replindex_scan_key (execReplication.c:55): one scan key per
// non-expression index key column, using the index opclass's own equality
// operator (opfamily member for the AM's COMPARE_EQ strategy), never the
// type's default equality.
fn build_replindex_scan_key(
    idxrel: &Relation<'_>,
    rel: &Relation<'_>,
    searchslot: &mut SlotData<'_>,
) -> PgResult<Vec<ScanKeyData>> {
    let form = idxrel.rd_index.as_ref().expect("index form");
    let mut keys = Vec::new();
    exectuples::slot_getallattrs(searchslot);
    for (i, &table_attno) in form.indkey.iter().take(form.indnkeyatts as usize).enumerate() {
        if table_attno == 0 {
            // XXX: Currently, we don't support expressions in the scan key.
            continue;
        }
        let att = rel.rd_att.attr((table_attno - 1) as usize);
        // Load the operator info (execReplication.c:92): rd_opcintype /
        // rd_opfamily are the opclass's input type and family.
        let optype = idxrel.rd_opcintype[i];
        let opfamily = idxrel.rd_opfamily[i];
        let eq_strategy = amapi::IndexAmTranslateCompareType(
            lsyscache::COMPARE_EQ,
            idxrel.rd_rel.relam,
            opfamily,
            false,
        )?;
        let operator = lsyscache::get_opfamily_member(opfamily, optype, optype, eq_strategy as i16)?;
        if operator == InvalidOid {
            return Err(Box::new(PgError::error(format!(
                "missing operator {eq_strategy}({optype},{optype}) in opfamily {opfamily}"
            ))));
        }
        let regop = lsyscache::get_opcode(operator)?;
        let mut key = ScanKeyData::empty();
        key.sk_attno = (i + 1) as i16;
        key.sk_strategy = eq_strategy;
        key.sk_func = fmgr_seams::fmgr_info::call(regop)?;
        key.sk_collation = idxrel.rd_indcollation.get(i).copied().unwrap_or(att.attcollation);
        let (isnull, value) = {
            let b = searchslot.base();
            (
                b.tts_isnull[(table_attno - 1) as usize],
                b.tts_values[(table_attno - 1) as usize],
            )
        };
        key.sk_argument = value;
        if isnull {
            key.sk_flags |= SK_ISNULL | SK_SEARCHNULL;
        }
        keys.push(key);
    }
    debug_assert!(!keys.is_empty());
    Ok(keys)
}

// RelationFindReplTupleByIndex (execReplication.c:179).
fn find_repl_tuple_by_index<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    idxoid: Oid,
    searchslot: &mut SlotData<'mcx>,
    outslot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    let idxrel = indexam::index_open(mcx, idxoid, types_rel::RowExclusiveLock)?;
    // execReplication.c:200: only the primary key / replica identity index
    // may skip the per-candidate equality check.
    let is_idx_safe_to_skip_duplicates =
        logicalrelation::get_relation_identity_or_pk(mcx, rel)? == idxoid;

    // InitDirtySnapshot (execReplication.c:199): an in-progress match
    // surfaces through dirty_xmin/dirty_xmax and is waited out, then the
    // scan is retried.
    let snap = Rc::new(types_snapshot::SnapshotData::sentinel(mcx, types_snapshot::SNAPSHOT_DIRTY));
    let keys = build_replindex_scan_key(&idxrel, rel, searchslot)?;

    let found = loop {
        let mut scan =
            indexam::index_beginscan(mcx, rel, &idxrel, snap.clone(), keys.len() as i32, 0)?;
        let mut kv = mcx::PgVec::new_in(mcx);
        for k in &keys {
            kv.push(k.clone());
        }
        indexam::index_rescan(&mut scan, Some(&kv), None)?;

        let mut found = false;
        let mut xwait = InvalidTransactionId;
        while indexam::index_getnext_slot(
            mcx,
            &mut scan,
            types_scan::sdir::ScanDirection::ForwardScanDirection,
            outslot,
        )? {
            // Avoid the expensive equality check if the index is the primary
            // key or replica identity index (execReplication.c:222).
            if !is_idx_safe_to_skip_duplicates && !tuples_equal(mcx, outslot, searchslot, rel)? {
                continue;
            }
            // ExecMaterializeSlot (execReplication.c:228): own the tuple
            // before the scan's pin goes away.
            exectuples::exec_materialize_slot(outslot, mcx)?;
            xwait = dirty_xwait(&snap);
            if xwait != InvalidTransactionId {
                break;
            }
            found = true;
            break;
        }

        if xwait != InvalidTransactionId {
            indexam::index_endscan(scan)?;
            lmgr::XactLockTableWait(xwait, None, None, types_storage::lock::XLTW_Oper::None)?;
            continue;
        }
        if found {
            // Lock the found tuple; on concurrent update/delete retry the scan
            // (should_refetch_tuple protocol).
            let tid = outslot.base().tts_tid;
            snapmgr::PushActiveSnapshot(&snapmgr::GetLatestSnapshot()?)?;
            let lockres = tableam::table_tuple_lock_for_repl(mcx, rel, &tid, outslot);
            snapmgr::PopActiveSnapshot()?;
            indexam::index_endscan(scan)?;
            match lockres? {
                LockOutcome::Ok => break true,
                LockOutcome::Retry => continue,
            }
        }
        indexam::index_endscan(scan)?;
        break false;
    };

    indexam::index_close(idxrel, types_rel::NoLock)?;
    Ok(found)
}

// execReplication.c:232: the in-progress inserter (xmin) or locker/deleter
// (xmax) the dirty scan saw on the matched tuple.
fn dirty_xwait(snap: &types_snapshot::SnapshotData<'_>) -> TransactionId {
    let xmin = snap.dirty_xmin.get();
    if xmin != InvalidTransactionId {
        xmin
    } else {
        snap.dirty_xmax.get()
    }
}

#[derive(Debug)]
pub(crate) enum LockOutcome {
    Ok,
    Retry,
}

// should_refetch_tuple (execReplication.c:135): map a table_tuple_lock result
// to the apply-side retry/error outcome. C logs the concurrent update/delete
// cases at LOG with 40001, raises "attempted to lock invisible tuple" for
// TM_Invisible, and "unexpected table_tuple_lock status: %u" (with the colon)
// for anything else.
pub(crate) fn should_refetch_tuple(
    res: tableam_real::TM_Result,
    moved_partitions: bool,
) -> PgResult<LockOutcome> {
    use tableam_real::TM_Result;
    match res {
        TM_Result::TM_Ok => Ok(LockOutcome::Ok),
        TM_Result::TM_Updated => {
            let msg = if moved_partitions {
                "tuple to be locked was already moved to another partition due to concurrent update, retrying"
            } else {
                "concurrent update, retrying"
            };
            ereport(LOG)
                .errcode(ERRCODE_T_R_SERIALIZATION_FAILURE)
                .errmsg(msg)
                .finish(loc("should_refetch_tuple"))?;
            Ok(LockOutcome::Retry)
        }
        TM_Result::TM_Deleted => {
            ereport(LOG)
                .errcode(ERRCODE_T_R_SERIALIZATION_FAILURE)
                .errmsg("concurrent delete, retrying")
                .finish(loc("should_refetch_tuple"))?;
            Ok(LockOutcome::Retry)
        }
        TM_Result::TM_Invisible => {
            ereport(ERROR)
                .errmsg("attempted to lock invisible tuple")
                .finish(loc("should_refetch_tuple"))?;
            unreachable!();
        }
        other => {
            ereport(ERROR)
                .errmsg(format!("unexpected table_tuple_lock status: {}", other as u32))
                .finish(loc("should_refetch_tuple"))?;
            unreachable!();
        }
    }
}

mod tableam {
    // table_tuple_lock with execReplication.c's should_refetch_tuple mapping.
    use super::*;
    use types_tuple::itemptr::ItemPointerData;

    pub(super) fn table_tuple_lock_for_repl<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        tid: &ItemPointerData,
        slot: &mut SlotData<'mcx>,
    ) -> PgResult<LockOutcome> {
        use tableam_real::{LockTupleMode, LockWaitPolicy, TM_FailureData, TM_Result};
        let mut tmfd = TM_FailureData::default();
        let snap = Some(snapmgr::GetActiveSnapshot());
        let cid = xact::GetCurrentCommandId(false)?;
        let res = tableam_real::table_tuple_lock(
            mcx,
            rel,
            tid,
            &snap,
            slot,
            cid,
            LockTupleMode::LockTupleExclusive,
            LockWaitPolicy::LockWaitBlock,
            0,
            &mut tmfd,
        )?;
        // TM_SelfModified is not reachable through the apply-side lock (the
        // worker never locks its own uncommitted tuple); C's should_refetch
        // has no such case. Keep pgrust's existing Ok mapping for it and route
        // everything else through the C-exact should_refetch_tuple.
        if let TM_Result::TM_SelfModified = res {
            return Ok(LockOutcome::Ok);
        }
        // tmfd.ctid is only filled for TM_Updated (C reads it in that arm
        // alone; ItemPointerGetOffsetNumber asserts on the zeroed one).
        let moved = matches!(res, TM_Result::TM_Updated)
            && ::types_tuple::ItemPointerIndicatesMovedPartitions(&tmfd.ctid);
        super::should_refetch_tuple(res, moved)
    }
}

// RelationFindReplTupleSeq (execReplication.c:355).
fn find_repl_tuple_seq<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    searchslot: &mut SlotData<'mcx>,
    outslot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    // InitDirtySnapshot (see the by-index variant).
    let snap = Rc::new(types_snapshot::SnapshotData::sentinel(mcx, types_snapshot::SNAPSHOT_DIRTY));
    let found = loop {
        let mut scan =
            tableam_real::table_beginscan(mcx, rel, Some(snap.clone()), 0, mcx::PgVec::new_in(mcx))?;
        let mut scanslot = tableam_real::table_slot_create(mcx, rel)?;

        let mut found = false;
        let mut xwait = InvalidTransactionId;
        while tableam_real::table_scan_getnextslot(
            mcx,
            &mut scan,
            types_scan::sdir::ScanDirection::ForwardScanDirection,
            &mut scanslot,
        )? {
            if !tuples_equal(mcx, &mut scanslot, searchslot, rel)? {
                continue;
            }
            // ExecCopySlot is a DEEP copy in C: materialize now, while the
            // datums still point into scanslot's pinned buffer.
            let natts = rel.rd_att.natts as usize;
            exectuples::exec_clear_tuple(outslot, mcx);
            exectuples::slot_getallattrs(&mut scanslot);
            for i in 0..natts {
                let (v, n) = {
                    let b = scanslot.base();
                    (b.tts_values[i], b.tts_isnull[i])
                };
                let base = outslot.base_mut();
                base.tts_values[i] = v;
                base.tts_isnull[i] = n;
            }
            outslot.base_mut().tts_tid = scanslot.base().tts_tid;
            exectuples::exec_store_virtual_tuple(outslot);
            exectuples::exec_materialize_slot(outslot, mcx)?;
            xwait = dirty_xwait(&snap);
            if xwait != InvalidTransactionId {
                break;
            }
            found = true;
            break;
        }
        // ExecDropSingleTupleTableSlot (execReplication.c:430): the scan
        // slot's own buffer pin goes with it.
        exectuples::exec_clear_tuple(&mut scanslot, mcx);

        if xwait != InvalidTransactionId {
            tableam_real::table_endscan(scan)?;
            lmgr::XactLockTableWait(xwait, None, None, types_storage::lock::XLTW_Oper::None)?;
            continue;
        }
        if found {
            let tid = outslot.base().tts_tid;
            snapmgr::PushActiveSnapshot(&snapmgr::GetLatestSnapshot()?)?;
            let lockres = tableam::table_tuple_lock_for_repl(mcx, rel, &tid, outslot);
            snapmgr::PopActiveSnapshot()?;
            tableam_real::table_endscan(scan)?;
            match lockres? {
                LockOutcome::Ok => break true,
                LockOutcome::Retry => continue,
            }
        }
        tableam_real::table_endscan(scan)?;
        break false;
    };
    Ok(found)
}


// FindReplTupleInLocalRel (worker.c:2915).
// TargetPrivilegesCheck (worker.c:2356): the subscription owner must hold the
// required privilege on the target relation to perform the given operation, and
// RLS is unsupported on the apply path (tablesync workers lack it too), so any
// relation with RLS enabled is refused for every command alike.
fn target_privileges_check<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    mode: u64,
) -> PgResult<()> {
    let relid = rel.rd_id;
    let aclresult = aclchk::pg_class_aclcheck(relid, miscinit::GetUserId(), mode)?;
    if aclresult != aclchk::ACLCHECK_OK {
        let relname = lsyscache::get_rel_name(mcx, relid)?;
        aclchk::aclcheck_error(
            aclresult,
            tablecmds::get_relkind_objtype(rel.rd_rel.relkind),
            relname.as_ref().map(|s| s.as_str()).unwrap_or(""),
        )?;
    }

    if rls::check_enable_rls(relid, InvalidOid, false)? == rls::CheckEnableRls::RlsEnabled {
        let username = miscinit::GetUserNameFromId(mcx, miscinit::GetUserId(), true)?;
        ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "user \"{}\" cannot replicate into relation with row-level security enabled: \"{}\"",
                username.as_ref().map(|s| s.as_str()).unwrap_or(""),
                rel.name()
            ))
            .finish(loc("target_privileges_check"))?;
    }
    Ok(())
}

fn find_repl_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    entry: &LogicalRepRelMapEntry,
    searchslot: &mut SlotData<'mcx>,
    outslot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    target_privileges_check(mcx, rel, types_nodes::parsenodes::ACL_SELECT)?;
    if entry.localindexoid != InvalidOid {
        find_repl_tuple_by_index(mcx, rel, entry.localindexoid, searchslot, outslot)
    } else {
        find_repl_tuple_seq(mcx, rel, searchslot, outslot)
    }
}

// ExecSimpleRelationInsert rendering (execReplication.c:562).
fn do_insert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<()> {
    target_privileges_check(mcx, rel, types_nodes::parsenodes::ACL_INSERT)?;
    execreplication::CheckCmdReplicaIdentity(mcx, rel, types_nodes::nodes_enums::CmdType::CMD_INSERT)?;

    let mut trig = apply_trig(rel)?;
    // BEFORE ROW INSERT triggers (execReplication.c:575); a NULL return means
    // "do nothing" for this row.
    if let Some(t) = trig.as_mut() {
        if t.td.trig_insert_before_row {
            let td = t.td.clone();
            let mut when =
                trigger::TriggerWhenEval { mcx, cache: &mut t.when, modified_cols: None };
            if !trigger::ExecBRInsertTriggers(mcx, rel, &td, &mut t.fmgr, &mut when, slot)? {
                return Ok(());
            }
        }
    }

    let mut generated_exprs = None;
    if rel.rd_att.constr.as_deref().is_some_and(|c| c.has_generated_stored) {
        nodemodifytable::exec_compute_stored_generated(mcx, &mut generated_exprs, None, rel, slot)?;
    }
    let mut check_exprs = None;
    let mut nn_exprs = None;
    if rel.rd_att.constr.is_some() {
        nodemodifytable::exec_constraints(mcx, &mut check_exprs, &mut nn_exprs, rel, slot, None, None)?;
    }
    // ExecPartitionCheck (execReplication.c:600): a row replicated straight
    // into a partition must satisfy its partition constraint.
    if rel.rd_rel.relispartition {
        let mut check_cache = None;
        if !execpartition::exec_partition_check(mcx, &mut check_cache, rel, slot)? {
            return Err(execpartition::partition_constraint_violation(mcx, rel, slot, None, None));
        }
    }

    tableam_real::simple_table_tuple_insert(mcx, rel, slot)?;

    // InitConflictIndexes (worker.c:2502) + ExecSimpleRelationInsert's
    // conflict arm (execReplication.c:614-651): the unique, non-deferrable
    // indexes are the arbiters; a potential conflict is checked after the
    // insert (no extra scan when conflicts are rare) and reported as
    // insert_exists instead of the plain unique-violation error.
    let mut index_state = execindexing::ExecOpenIndices(mcx, rel, false)?;
    let conflict_indexes = crate::conflict::init_conflict_indexes(&index_state);
    let mut conflict = false;
    let recheck_indexes = if index_state.num_indices() > 0 {
        let eval_cx = mcx::MemoryContext::new("ApplyIndexEval");
        // ExecSimpleRelationInsert (execReplication.c:630): update = false.
        let r = execindexing::ExecInsertIndexTuples(
            mcx,
            eval_cx.mcx(),
            &mut index_state,
            rel,
            slot,
            None,
            !conflict_indexes.is_empty(),
            Some(&mut conflict),
            &conflict_indexes,
            false,
        )?;
        r.to_vec()
    } else {
        Vec::new()
    };
    if conflict {
        crate::conflict::check_and_report_conflict(
            mcx,
            rel,
            &mut index_state,
            crate::conflict::ConflictType::InsertExists,
            &recheck_indexes,
            &conflict_indexes,
            None,
            slot,
        )?;
    }
    execindexing::ExecCloseIndices(index_state)?;

    // AFTER ROW INSERT triggers (execReplication.c:629). C passes no
    // TransitionCaptureState here (its own comment: after-statement triggers
    // are not fired by replication yet).
    if let Some(t) = trig.as_mut() {
        let td = t.td.clone();
        let new_tid = slot.base().tts_tid;
        let mut when = trigger::TriggerWhenEval { mcx, cache: &mut t.when, modified_cols: None };
        trigger::ExecARInsertTriggers(
            mcx,
            rel,
            Some(&td),
            new_tid,
            &recheck_indexes,
            None,
            Some(&mut when),
            None,
        )?;
    }
    Ok(())
}

// worker.c:2606-2626: populate updatedCols so per-column triggers can fire.
// Encoded as ExecGetAllUpdatedCols (attno - FirstLowInvalidHeapAttributeNumber).
fn apply_updated_cols<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    entry: &LogicalRepRelMapEntry,
    newtup: &LogicalRepTupleData,
) -> PgResult<types_nodes::Bitmapset<'mcx>> {
    use types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
    let mut cols = types_nodes::Bitmapset::empty();
    for i in 0..rel.rd_att.natts as usize {
        let att = rel.rd_att.attr(i);
        let remote = entry.attrmap.get(i).copied().unwrap_or(-1);
        if att.attisdropped || remote < 0 {
            continue;
        }
        let m = remote as usize;
        // worker.c:2632: same protocol-violation check as slot_store_data.
        tuple_column_check(m, newtup.ncols)?;
        if newtup.colstatus[m] != LOGICALREP_COLUMN_UNCHANGED {
            cols.add_member(mcx, (i as i32 + 1) - FirstLowInvalidHeapAttributeNumber)?;
        }
    }
    Ok(cols)
}

// ExecSimpleRelationUpdate rendering (execReplication.c:651).
fn do_update<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    searchslot: &mut SlotData<'mcx>,
    slot: &mut SlotData<'mcx>,
    modified_cols: Option<&types_nodes::Bitmapset<'mcx>>,
) -> PgResult<()> {
    use tableam_vocab::TU_UpdateIndexes;

    target_privileges_check(mcx, rel, types_nodes::parsenodes::ACL_UPDATE)?;
    execreplication::CheckCmdReplicaIdentity(mcx, rel, types_nodes::nodes_enums::CmdType::CMD_UPDATE)?;

    let mut trig = apply_trig(rel)?;
    // ExecGetAllUpdatedCols: the remote-changed columns (worker.c:2606-2626)
    // plus the generated columns ExecInitGenerated(CMD_UPDATE) recomputes;
    // the trigger legs, the generated-column evaluation and the index
    // maintenance all read this one set in C.
    let mut all_updated_cols = match modified_cols {
        Some(c) => c.clone_in(mcx)?,
        None => types_nodes::Bitmapset::empty(),
    };
    nodemodifytable::add_generated_extra_updated_cols(
        mcx,
        rel,
        trig.as_ref().is_some_and(|t| t.td.trig_update_before_row),
        &mut all_updated_cols,
    )?;
    let all_updated_cols = &all_updated_cols;
    // BEFORE ROW UPDATE triggers (execReplication.c:685); a NULL return means
    // "do nothing" for this row. The old row is the locked tuple already
    // fetched into searchslot (C GetTupleForTrigger by its tid).
    if let Some(t) = trig.as_mut() {
        if t.td.trig_update_before_row {
            let td = t.td.clone();
            let mut when = trigger::TriggerWhenEval {
                mcx,
                cache: &mut t.when,
                modified_cols: Some(all_updated_cols),
            };
            if !trigger::ExecBRUpdateTriggers(
                mcx,
                rel,
                &td,
                &mut t.fmgr,
                &mut when,
                searchslot,
                slot,
                Some(all_updated_cols),
            )? {
                exectuples::exec_clear_tuple(searchslot, mcx);
                return Ok(());
            }
        }
    }

    let mut generated_exprs = None;
    if rel.rd_att.constr.as_deref().is_some_and(|c| c.has_generated_stored) {
        // ExecInitGenerated(CMD_UPDATE): the worker's updatedCols filter the
        // recomputed generated columns unless a BEFORE ROW UPDATE trigger runs.
        let gen_filter = if trig.as_ref().is_some_and(|t| t.td.trig_update_before_row) {
            None
        } else {
            modified_cols
        };
        nodemodifytable::exec_compute_stored_generated(
            mcx,
            &mut generated_exprs,
            gen_filter,
            rel,
            slot,
        )?;
    }
    let mut check_exprs = None;
    let mut nn_exprs = None;
    if rel.rd_att.constr.is_some() {
        nodemodifytable::exec_constraints(mcx, &mut check_exprs, &mut nn_exprs, rel, slot, None, None)?;
    }
    // ExecPartitionCheck (execReplication.c:706): the updated row must still
    // satisfy the partition's constraint.
    if rel.rd_rel.relispartition {
        let mut check_cache = None;
        if !execpartition::exec_partition_check(mcx, &mut check_cache, rel, slot)? {
            return Err(execpartition::partition_constraint_violation(
                mcx,
                rel,
                slot,
                modified_cols,
                None,
            ));
        }
    }

    let otid = searchslot.base().tts_tid;
    let snap = Some(snapmgr::GetActiveSnapshot());
    let mut update_indexes = TU_UpdateIndexes::TU_None;
    tableam_real::simple_table_tuple_update(mcx, rel, &otid, slot, &snap, &mut update_indexes)?;

    let mut recheck_indexes: Vec<Oid> = Vec::new();
    if !matches!(update_indexes, TU_UpdateIndexes::TU_None) {
        let only_summarizing = matches!(update_indexes, TU_UpdateIndexes::TU_Summarizing);
        // InitConflictIndexes (worker.c:2735) + ExecSimpleRelationUpdate's
        // conflict arm (execReplication.c:717-730): see do_insert.
        let mut index_state = execindexing::ExecOpenIndices(mcx, rel, false)?;
        let conflict_indexes = crate::conflict::init_conflict_indexes(&index_state);
        let mut conflict = false;
        if index_state.num_indices() > 0 {
            let eval_cx = mcx::MemoryContext::new("ApplyIndexEval");
            // ExecSimpleRelationUpdate (execReplication.c:720): update = true
            // with ExecGetAllUpdatedCols.
            let r = execindexing::ExecInsertIndexTuples(
                mcx,
                eval_cx.mcx(),
                &mut index_state,
                rel,
                slot,
                Some(all_updated_cols),
                !conflict_indexes.is_empty(),
                Some(&mut conflict),
                &conflict_indexes,
                only_summarizing,
            )?;
            recheck_indexes = r.to_vec();
        }
        if conflict {
            crate::conflict::check_and_report_conflict(
                mcx,
                rel,
                &mut index_state,
                crate::conflict::ConflictType::UpdateExists,
                &recheck_indexes,
                &conflict_indexes,
                Some(searchslot),
                slot,
            )?;
        }
        execindexing::ExecCloseIndices(index_state)?;
    }

    // AFTER ROW UPDATE triggers (execReplication.c:715): C ExecARUpdateTriggers
    // reads ExecGetAllUpdatedCols off the estate that worker.c:2606 filled.
    if let Some(t) = trig.as_mut() {
        let td = t.td.clone();
        let new_tid = slot.base().tts_tid;
        let mut when = trigger::TriggerWhenEval {
            mcx,
            cache: &mut t.when,
            modified_cols: Some(all_updated_cols),
        };
        trigger::ExecARUpdateTriggers(
            mcx,
            rel,
            Some(&td),
            None,
            None,
            Some(otid),
            Some(new_tid),
            &recheck_indexes,
            None,
            Some(&mut when),
            false,
            None,
            None,
            Some(all_updated_cols),
        )?;
    }
    // ExecDropSingleTupleTableSlot(localslot) in the callers (worker.c:2760):
    // release the locked tuple's buffer pin now rather than at end of
    // transaction.
    exectuples::exec_clear_tuple(searchslot, mcx);
    Ok(())
}

// ExecSimpleRelationDelete rendering (execReplication.c:734).
fn do_delete<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    searchslot: &mut SlotData<'mcx>,
) -> PgResult<()> {
    target_privileges_check(mcx, rel, types_nodes::parsenodes::ACL_DELETE)?;
    execreplication::CheckCmdReplicaIdentity(mcx, rel, types_nodes::nodes_enums::CmdType::CMD_DELETE)?;

    let mut trig = apply_trig(rel)?;
    // BEFORE ROW DELETE triggers (execReplication.c:753); a NULL return
    // suppresses the delete.
    if let Some(t) = trig.as_mut() {
        if t.td.trig_delete_before_row {
            let td = t.td.clone();
            let mut when =
                trigger::TriggerWhenEval { mcx, cache: &mut t.when, modified_cols: None };
            if !trigger::ExecBRDeleteTriggers(mcx, rel, &td, &mut t.fmgr, &mut when, searchslot)? {
                exectuples::exec_clear_tuple(searchslot, mcx);
                return Ok(());
            }
        }
    }

    let tid = searchslot.base().tts_tid;
    let snap = Some(snapmgr::GetActiveSnapshot());
    tableam_real::simple_table_tuple_delete(mcx, rel, &tid, &snap)?;

    // AFTER ROW DELETE triggers (execReplication.c:757).
    if let Some(t) = trig.as_mut() {
        let td = t.td.clone();
        let mut when = trigger::TriggerWhenEval { mcx, cache: &mut t.when, modified_cols: None };
        trigger::ExecARDeleteTriggers(
            mcx,
            rel,
            Some(&td),
            tid,
            None,
            Some(&mut when),
            false,
            None,
        )?;
    }
    exectuples::exec_clear_tuple(searchslot, mcx);
    Ok(())
}

// The routed operation for apply_handle_tuple_routing; Update carries the
// new-tuple data (the search tuple already sits in the caller's root slot).
enum RoutedOp<'a> {
    Insert,
    Update(&'a LogicalRepTupleData),
    Delete,
}

// apply_handle_tuple_routing (worker.c:3130): route the remote tuple (in the
// partitioned root's layout) to its leaf partition and apply the operation
// there. Renderings vs C:
// - PartitionTupleRouting is the execpartition port; there is no
//   ModifyTableState scaffold (find_partition takes the slot directly).
// - The per-partition LogicalRepRelMapEntry is rebuilt per call
//   (logicalrep_partition_open rendering; C caches it in LogicalRepPartMap).
// - Slot conversions run in the worker's apply context; per-tuple datum
//   lifetimes follow the caller's InFuncs keep-alive discipline (the
//   converted slots copy datum pointers whose storage the caller owns).
fn apply_handle_tuple_routing<'mcx>(
    mcx: Mcx<'mcx>,
    entry: &LogicalRepRelMapEntry,
    rel: &Relation<'mcx>,
    remoteslot: &mut SlotData<'mcx>,
    op: RoutedOp<'_>,
) -> PgResult<()> {
    // C edata->estate's es_partition_directory (create_edata_for_relation's
    // CreateExecutorState; freed by finish_edata).
    let mut partition_directory: Option<execpartition::PartitionDirectory<'mcx>> = None;
    let mut proute =
        execpartition::PartitionTupleRouting::new(mcx, rel, &mut partition_directory)?;
    // C's per-tuple context for routing-key evaluation.
    let eval_cx = mcx::MemoryContext::new("ApplyTupleRoutingEval");

    let idx = proute.find_partition(remoteslot, eval_cx.mcx(), &mut partition_directory)?;
    let partrel = proute.leaf_rel(idx).alias();
    // CheckSubscriptionRelkind (worker.c:3161): the partition set can change,
    // so CREATE/ALTER SUBSCRIPTION-time checks are insufficient.
    logicalrelation::check_relkind(
        partrel.rd_rel.relkind as u8,
        &entry.remoterel.nspname,
        &entry.remoterel.relname,
    )?;
    // Same reason for the AM gate: a columnar leaf can be attached after the
    // CREATE/REFRESH-time CheckSubscriptionRelam check ran.
    logicalrelation::check_target_am(
        partrel.rd_rel.relam,
        &entry.remoterel.nspname,
        &entry.remoterel.relname,
    )?;

    // Convert the tuple to the partition's rowtype if needed (worker.c:3168).
    let root_to_leaf: Option<Vec<i16>> = proute.leaf_attrmap(idx).map(|m| m.to_vec());
    let mut remoteslot_part = tableam_real::table_slot_create(mcx, &partrel)?;
    match root_to_leaf.as_deref() {
        Some(map) => exectuples::execute_attr_map_slot(map, remoteslot, &mut remoteslot_part, mcx),
        None => exectuples::exec_copy_slot(&mut remoteslot_part, remoteslot, mcx, mcx)?,
    }

    match op {
        RoutedOp::Insert => do_insert(mcx, &partrel, &mut remoteslot_part),
        RoutedOp::Delete => {
            let part_entry = logicalrelation::logicalrep_partition_open(
                mcx,
                entry,
                &partrel,
                root_to_leaf.as_deref(),
            )?;
            check_relation_updatable(mcx, &partrel, &part_entry)?;

            let mut localslot = tableam_real::table_slot_create(mcx, &partrel)?;
            let found =
                find_repl_tuple(mcx, &partrel, &part_entry, &mut remoteslot_part, &mut localslot)?;
            if found {
                let (localxmin, ctsdata) = get_tuple_transaction_info(&localslot)?;
                if let Some((localts, localorigin)) = ctsdata {
                    if localorigin != origin::replorigin_session_origin() {
                        report_origin_differs(
                            mcx,
                            &partrel,
                            &part_entry,
                            &mut remoteslot_part,
                            &mut localslot,
                            None,
                            localxmin,
                            localorigin,
                            localts,
                        )?;
                    }
                }
                do_delete(mcx, &partrel, &mut localslot)?;
            } else {
                // The tuple to be deleted could not be found (worker.c:2911).
                report_row_missing(
                    mcx,
                    &partrel,
                    &part_entry,
                    &mut remoteslot_part,
                    &mut localslot,
                    None,
                )?;
            }
            Ok(())
        }
        RoutedOp::Update(newtup) => {
            let part_entry = logicalrelation::logicalrep_partition_open(
                mcx,
                entry,
                &partrel,
                root_to_leaf.as_deref(),
            )?;
            check_relation_updatable(mcx, &partrel, &part_entry)?;

            let mut localslot = tableam_real::table_slot_create(mcx, &partrel)?;
            let found =
                find_repl_tuple(mcx, &partrel, &part_entry, &mut remoteslot_part, &mut localslot)?;
            if !found {
                // The tuple to be updated could not be found (worker.c:3083).
                report_row_missing(
                    mcx,
                    &partrel,
                    &part_entry,
                    &mut remoteslot_part,
                    &mut localslot,
                    Some(newtup),
                )?;
                return Ok(());
            }
            let (localxmin, ctsdata) = get_tuple_transaction_info(&localslot)?;
            if let Some((localts, localorigin)) = ctsdata {
                if localorigin != origin::replorigin_session_origin() {
                    report_origin_differs(
                        mcx,
                        &partrel,
                        &part_entry,
                        &mut remoteslot_part,
                        &mut localslot,
                        Some(newtup),
                        localxmin,
                        localorigin,
                        localts,
                    )?;
                }
            }

            // Apply the update to the local tuple (worker.c:3253).
            let mut modfuncs = InFuncs::new(partrel.rd_att.natts as usize);
            let mut newslot = tableam_real::table_slot_create(mcx, &partrel)?;
            slot_modify_data(
                mcx, &mut newslot, &mut localslot, &part_entry, &partrel, newtup, &mut modfuncs,
            )?;

            // Does the updated tuple still satisfy the current partition's
            // constraint (worker.c:3264)?
            let mut check_cache = None;
            if !partrel.rd_rel.relispartition
                || execpartition::exec_partition_check(mcx, &mut check_cache, &partrel, &mut newslot)?
            {
                // Yes: simply UPDATE the partition.
                let updated = apply_updated_cols(mcx, &partrel, &part_entry, newtup)?;
                do_update(mcx, &partrel, &mut localslot, &mut newslot, Some(&updated))
            } else {
                // Move the tuple into the new partition (worker.c:3285):
                // DELETE from the old partition, re-route via the root, and
                // INSERT into the new one.
                let mut rootslot = tableam_real::table_slot_create(mcx, rel)?;
                match root_to_leaf.as_deref() {
                    Some(_) => {
                        let leaf_to_root =
                            tupdesc::build_attrmap_by_name(mcx, &partrel.rd_att, &rel.rd_att)?;
                        exectuples::execute_attr_map_slot(
                            &leaf_to_root, &mut newslot, &mut rootslot, mcx,
                        );
                    }
                    None => exectuples::exec_copy_slot(&mut rootslot, &mut newslot, mcx, mcx)?,
                }

                let new_idx =
                    proute.find_partition(&mut rootslot, eval_cx.mcx(), &mut partition_directory)?;
                let newpartrel = proute.leaf_rel(new_idx).alias();
                logicalrelation::check_relkind(
                    newpartrel.rd_rel.relkind as u8,
                    &entry.remoterel.nspname,
                    &entry.remoterel.relname,
                )?;
                logicalrelation::check_target_am(
                    newpartrel.rd_rel.relam,
                    &entry.remoterel.nspname,
                    &entry.remoterel.relname,
                )?;

                // DELETE old tuple found in the old partition.
                do_delete(mcx, &partrel, &mut localslot)?;

                // Convert the replacement tuple to the destination partition's
                // rowtype and INSERT.
                let new_map: Option<Vec<i16>> =
                    proute.leaf_attrmap(new_idx).map(|m| m.to_vec());
                let mut newslot_part = tableam_real::table_slot_create(mcx, &newpartrel)?;
                match new_map.as_deref() {
                    Some(map) => exectuples::execute_attr_map_slot(
                        map, &mut rootslot, &mut newslot_part, mcx,
                    ),
                    None => {
                        exectuples::exec_copy_slot(&mut newslot_part, &mut rootslot, mcx, mcx)?
                    }
                }
                do_insert(mcx, &newpartrel, &mut newslot_part)
            }
        }
    }
}

// apply_handle_insert (worker.c:2388).
fn apply_handle_insert(mcx: Mcx<'static>, r: &mut Reader<'_>) -> PgResult<()> {
    begin_replication_step(mcx)?;

    let (relid, newtup) = logicalproto::logicalrep_read_insert(r)?;
    let subid = my_sub(|s| s.oid);
    let (entry, rel) =
        logicalrelation::logicalrep_rel_open(mcx, relid, types_rel::RowExclusiveLock, subid)?;
    if !should_apply_changes_for_rel(&entry)? {
        logicalrelation::logicalrep_rel_close(rel, types_rel::RowExclusiveLock)?;
        return end_replication_step();
    }

    // Make sure that any user-supplied code runs as the table owner, unless
    // the user has opted out of that behavior (worker.c:2427).
    let ucxt = maybe_switch_to_table_owner(mcx, rel.rd_rel.relowner)?;

    // Set relation for error callback (worker.c:2442).
    set_apply_error_context_rel(Some(&entry.remoterel));

    // Prepare to catch AFTER triggers (create_edata_for_relation /
    // finish_edata, worker.c:512/554): the queue is opened and drained around
    // each apply operation.
    trigger::AfterTriggerBeginQuery();

    // Keep the input-function cache alive past do_insert: flinfo-owned
    // scratch backs the by-ref datums in the slot (see InFuncs).
    let mut infuncs = InFuncs::new(rel.rd_att.natts as usize);
    let mut remoteslot = tableam_real::table_slot_create(mcx, &rel)?;
    slot_store_data(mcx, &mut remoteslot, &entry, &rel, &newtup, &mut infuncs)?;
    // Keep-alive for the same reason as infuncs: default datums may live in
    // flinfo-owned scratch until do_insert materializes the tuple.
    let _default_exprs = slot_fill_defaults(mcx, &entry, &rel, &mut remoteslot)?;

    // For a partitioned table, insert the tuple into a partition
    // (worker.c:2448).
    if rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
        apply_handle_tuple_routing(mcx, &entry, &rel, &mut remoteslot, RoutedOp::Insert)?;
    } else {
        do_insert(mcx, &rel, &mut remoteslot)?;
    }

    trigger::AfterTriggerEndQuery(None)?;

    // Reset relation for error callback (worker.c:2473).
    set_apply_error_context_rel(None);

    restore_user_context(&ucxt)?;

    logicalrelation::logicalrep_rel_close(rel, types_rel::NoLock)?;
    end_replication_step()
}

// apply_handle_update (worker.c:2545).
fn apply_handle_update(mcx: Mcx<'static>, r: &mut Reader<'_>) -> PgResult<()> {
    begin_replication_step(mcx)?;

    let upd = logicalproto::logicalrep_read_update(r)?;
    let subid = my_sub(|s| s.oid);
    let (entry, rel) =
        logicalrelation::logicalrep_rel_open(mcx, upd.relid, types_rel::RowExclusiveLock, subid)?;
    if !should_apply_changes_for_rel(&entry)? {
        logicalrelation::logicalrep_rel_close(rel, types_rel::RowExclusiveLock)?;
        return end_replication_step();
    }

    // Set relation for error callback (worker.c:2595).
    set_apply_error_context_rel(Some(&entry.remoterel));

    check_relation_updatable(mcx, &rel, &entry)?;

    // Make sure that any user-supplied code runs as the table owner, unless
    // the user has opted out of that behavior (worker.c:2594).
    let ucxt = maybe_switch_to_table_owner(mcx, rel.rd_rel.relowner)?;

    // Prepare to catch AFTER triggers (create_edata_for_relation).
    trigger::AfterTriggerBeginQuery();

    // worker.c:2621: updatedCols (and its per-column protocol check) is
    // built before the search tuple is converted.
    let updated = apply_updated_cols(mcx, &rel, &entry, &upd.newtup)?;

    let mut infuncs = InFuncs::new(rel.rd_att.natts as usize);
    let mut remoteslot = tableam_real::table_slot_create(mcx, &rel)?;
    let searchtup = if upd.has_oldtuple {
        upd.oldtup.as_ref().expect("oldtuple present")
    } else {
        &upd.newtup
    };
    slot_store_data(mcx, &mut remoteslot, &entry, &rel, searchtup, &mut infuncs)?;

    // For a partitioned table, apply the update to the partition the search
    // tuple routes to (worker.c:2711).
    if rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
        apply_handle_tuple_routing(
            mcx,
            &entry,
            &rel,
            &mut remoteslot,
            RoutedOp::Update(&upd.newtup),
        )?;
        trigger::AfterTriggerEndQuery(None)?;
        set_apply_error_context_rel(None);
        restore_user_context(&ucxt)?;
        logicalrelation::logicalrep_rel_close(rel, types_rel::NoLock)?;
        return end_replication_step();
    }

    let mut localslot = tableam_real::table_slot_create(mcx, &rel)?;
    let found = find_repl_tuple(mcx, &rel, &entry, &mut remoteslot, &mut localslot)?;

    if found {
        // Report the conflict if the tuple was modified by a different origin
        // (worker.c:2696): commit-ts data present and origin != session's.
        let (localxmin, ctsdata) = get_tuple_transaction_info(&localslot)?;
        if let Some((localts, localorigin)) = ctsdata {
            if localorigin != origin::replorigin_session_origin() {
                report_origin_differs(
                    mcx,
                    &rel,
                    &entry,
                    &mut remoteslot,
                    &mut localslot,
                    Some(&upd.newtup),
                    localxmin,
                    localorigin,
                    localts,
                )?;
            }
        }

        // A second cache: slot_modify_data's writes reuse per-column scratch,
        // which would invalidate remoteslot's datums mid-use otherwise.
        let mut modfuncs = InFuncs::new(rel.rd_att.natts as usize);
        let mut newslot = tableam_real::table_slot_create(mcx, &rel)?;
        slot_modify_data(mcx, &mut newslot, &mut localslot, &entry, &rel, &upd.newtup, &mut modfuncs)?;
        do_update(mcx, &rel, &mut localslot, &mut newslot, Some(&updated))?;
    } else {
        // The tuple to be updated could not be found (worker.c:2740).
        report_row_missing(mcx, &rel, &entry, &mut remoteslot, &mut localslot, Some(&upd.newtup))?;
    }

    trigger::AfterTriggerEndQuery(None)?;

    // Reset relation for error callback (worker.c:2661).
    set_apply_error_context_rel(None);

    restore_user_context(&ucxt)?;

    logicalrelation::logicalrep_rel_close(rel, types_rel::NoLock)?;
    end_replication_step()
}

// apply_handle_delete (worker.c:2753).
fn apply_handle_delete(mcx: Mcx<'static>, r: &mut Reader<'_>) -> PgResult<()> {
    begin_replication_step(mcx)?;

    let (relid, oldtup) = logicalproto::logicalrep_read_delete(r)?;
    let subid = my_sub(|s| s.oid);
    let (entry, rel) =
        logicalrelation::logicalrep_rel_open(mcx, relid, types_rel::RowExclusiveLock, subid)?;
    if !should_apply_changes_for_rel(&entry)? {
        logicalrelation::logicalrep_rel_close(rel, types_rel::RowExclusiveLock)?;
        return end_replication_step();
    }

    // Set relation for error callback (worker.c:2804).
    set_apply_error_context_rel(Some(&entry.remoterel));

    check_relation_updatable(mcx, &rel, &entry)?;

    // Make sure that any user-supplied code runs as the table owner, unless
    // the user has opted out of that behavior (worker.c:2798).
    let ucxt = maybe_switch_to_table_owner(mcx, rel.rd_rel.relowner)?;

    // Prepare to catch AFTER triggers (create_edata_for_relation).
    trigger::AfterTriggerBeginQuery();

    let mut infuncs = InFuncs::new(rel.rd_att.natts as usize);
    let mut remoteslot = tableam_real::table_slot_create(mcx, &rel)?;
    slot_store_data(mcx, &mut remoteslot, &entry, &rel, &oldtup, &mut infuncs)?;

    // For a partitioned table, apply the delete in the partition the search
    // tuple routes to (worker.c:2864).
    if rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
        apply_handle_tuple_routing(mcx, &entry, &rel, &mut remoteslot, RoutedOp::Delete)?;
        trigger::AfterTriggerEndQuery(None)?;
        set_apply_error_context_rel(None);
        restore_user_context(&ucxt)?;
        logicalrelation::logicalrep_rel_close(rel, types_rel::NoLock)?;
        return end_replication_step();
    }

    let mut localslot = tableam_real::table_slot_create(mcx, &rel)?;
    let found = find_repl_tuple(mcx, &rel, &entry, &mut remoteslot, &mut localslot)?;

    if found {
        // Report the conflict if the tuple was modified by a different origin
        // (worker.c:2879).
        let (localxmin, ctsdata) = get_tuple_transaction_info(&localslot)?;
        if let Some((localts, localorigin)) = ctsdata {
            if localorigin != origin::replorigin_session_origin() {
                report_origin_differs(
                    mcx,
                    &rel,
                    &entry,
                    &mut remoteslot,
                    &mut localslot,
                    None,
                    localxmin,
                    localorigin,
                    localts,
                )?;
            }
        }

        do_delete(mcx, &rel, &mut localslot)?;
    } else {
        // The tuple to be deleted could not be found (worker.c:2908).
        report_row_missing(mcx, &rel, &entry, &mut remoteslot, &mut localslot, None)?;
    }

    trigger::AfterTriggerEndQuery(None)?;

    // Reset relation for error callback (worker.c:2846).
    set_apply_error_context_rel(None);

    restore_user_context(&ucxt)?;

    logicalrelation::logicalrep_rel_close(rel, types_rel::NoLock)?;
    end_replication_step()
}

// apply_handle_truncate (worker.c:3257). Even if the publisher used CASCADE,
// C explicitly replays without further cascading (DROP_RESTRICT).
// TargetPrivilegesCheck (ACL_TRUNCATE) runs for the target and for every
// truncated partition (worker.c:3297, :3335); ExecuteTruncateGuts switches
// to each table's owner exactly when the subscription does not run as its
// owner (worker.c:3351).
fn apply_handle_truncate(mcx: Mcx<'static>, r: &mut Reader<'_>) -> PgResult<()> {
    begin_replication_step(mcx)?;

    let (remote_relids, _cascade, restart_seqs) = logicalproto::logicalrep_read_truncate(r)?;
    let subid = my_sub(|s| s.oid);

    let mut rels: Vec<Relation<'static>> = Vec::new();
    let mut relids: Vec<Oid> = Vec::new();
    let mut relids_logged: Vec<Oid> = Vec::new();

    for relid in remote_relids {
        let (entry, rel) = logicalrelation::logicalrep_rel_open(
            mcx,
            relid,
            types_rel::AccessExclusiveLock,
            subid,
        )?;
        if !should_apply_changes_for_rel(&entry)? {
            logicalrelation::logicalrep_rel_close(rel, types_rel::AccessExclusiveLock)?;
            continue;
        }
        target_privileges_check(mcx, &rel, types_nodes::parsenodes::ACL_TRUNCATE)?;
        if heapam::relation_is_logically_logged(&rel) {
            relids_logged.push(rel.rd_id);
        }
        relids.push(rel.rd_id);
        let is_partitioned = rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE;
        let parentid = rel.rd_id;
        rels.push(rel);

        // Truncate partitions if we got a message to truncate a partitioned
        // table (worker.c:3277): fan out to all inheritors.
        if is_partitioned {
            let children =
                pg_inherits::find_all_inheritors(mcx, parentid, types_rel::AccessExclusiveLock)?;
            for &childrelid in children.iter() {
                if relids.contains(&childrelid) {
                    continue;
                }
                // find_all_inheritors already got the lock.
                let childrel = table::table_open(mcx, childrelid, types_rel::NoLock)?;
                // Ignore temp tables of other backends, as ExecuteTruncate
                // does (worker.c:3299).
                if childrel.is_other_temp() {
                    table::table_close(childrel, types_rel::AccessExclusiveLock)?;
                    continue;
                }
                target_privileges_check(mcx, &childrel, types_nodes::parsenodes::ACL_TRUNCATE)?;
                if heapam::relation_is_logically_logged(&childrel) {
                    relids_logged.push(childrelid);
                }
                relids.push(childrelid);
                rels.push(childrel);
            }
        }
    }

    if !rels.is_empty() {
        tablecmds::ExecuteTruncateGuts(
            mcx,
            &mut rels,
            &mut relids,
            &mut relids_logged,
            types_nodes::parsenodes::DropBehavior::DROP_RESTRICT,
            restart_seqs,
            // MySubscription->runasowner says whether replication actions run
            // as the subscription owner; the last argument tells the truncate
            // whether to switch to the table owner — exactly opposite
            // conditions (worker.c:3341).
            !my_sub(|s| s.runasowner),
        )?;
    }

    for rel in rels {
        logicalrelation::logicalrep_rel_close(rel, types_rel::NoLock)?;
    }

    end_replication_step()
}

#[cfg(test)]
mod tests {
    // The TEXT-column arm of slot_store_datum now runs pg_verifymbstr against
    // the subscriber's database encoding before handing publisher-supplied
    // bytes to the type input function (idx 76). slot_store_datum itself needs
    // a live backend (mcx, fmgr, catalogs), so guard the exact predicate it
    // relies on: under a UTF-8 database, invalid multibyte sequences must
    // produce a catchable error rather than flow through to from_utf8_unchecked
    // sinks, while valid text passes. SQL_ASCII (byte-transparent) is left to
    // mbutils' own coverage.
    #[test]
    fn text_column_bytes_are_encoding_verified() {
        use wchar::PG_UTF8;

        // Valid UTF-8 (ASCII and a multibyte codepoint) is accepted.
        assert!(mbutils::pg_verify_mbstr(PG_UTF8, b"hello", false).unwrap());
        assert!(mbutils::pg_verify_mbstr(PG_UTF8, "ol\u{00e9}".as_bytes(), false).unwrap());

        // A lone 0xFF and a truncated multibyte sequence are rejected with a
        // catchable error instead of becoming a poisoned datum.
        let err = mbutils::pg_verify_mbstr(PG_UTF8, b"bad\xff", false)
            .err()
            .unwrap();
        assert_eq!(
            err.sqlstate(),
            types_error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE
        );
        let err = mbutils::pg_verify_mbstr(PG_UTF8, b"bad\xe2\x82", false)
            .err()
            .unwrap();
        assert_eq!(
            err.sqlstate(),
            types_error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE
        );
    }
}
