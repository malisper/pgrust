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

use datum::Datum;
use elog::ereport;
use logicalproto::{
    LogicalRepTupleData, Reader, LOGICALREP_COLUMN_BINARY, LOGICALREP_COLUMN_TEXT,
    LOGICALREP_COLUMN_UNCHANGED,
};
use logicalrelation::LogicalRepRelMapEntry;
use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::{
    PgResult, ERRCODE_INVALID_BINARY_REPRESENTATION, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_PROTOCOL_VIOLATION, ERROR, LOG,
};
use types_rel::Relation;
use types_scan::scankey::{ScanKeyData, BTEqualStrategyNumber, SK_ISNULL, SK_SEARCHNULL};
use types_slot::SlotData;

use walreceiver::client::PgConn;

use crate::{loc, my_sub, IN_REMOTE_TRANSACTION, REMOTE_FINAL_LSN};

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

// should_apply_changes_for_rel (worker.c:461), leader-apply arm. Non-READY
// states belong to tablesync (inc E) and refuse loudly rather than desync.
fn should_apply_changes_for_rel(entry: &LogicalRepRelMapEntry) -> bool {
    const SUBREL_STATE_READY: u8 = b'r';
    const SUBREL_STATE_SYNCDONE: u8 = b's';
    if crate::tablesync::AM_TABLESYNC_WORKER.with(std::cell::Cell::get) {
        // The tablesync worker applies only its own table's changes
        // (worker.c:461); localreloid match is implied by the relmap entry.
        let myrelid = launcher::worker_snapshot(launcher::my_worker_slot().expect("attached"))
            .map(|w| w.relid)
            .unwrap_or(types_core::InvalidOid);
        return entry.localreloid == myrelid;
    }
    match entry.state {
        SUBREL_STATE_READY => true,
        SUBREL_STATE_SYNCDONE => entry.statelsn <= REMOTE_FINAL_LSN.get(),
        // INIT/DATASYNC/FINISHEDCOPY/SYNCWAIT/CATCHUP: the tablesync worker
        // owns those changes; the leader skips them.
        _ => false,
    }
}

// apply_dispatch (worker.c:3368). C intercepts streamed chunks inside each
// data-message handler (handle_streamed_transaction at their entry); doing it
// once here is equivalent — inside a streamed chunk every data message spools
// to the transaction's file instead of applying.
pub(crate) fn apply_dispatch(mcx: Mcx<'static>, conn: &mut PgConn, buf: &[u8]) -> PgResult<()> {
    if buf.is_empty() {
        return Ok(());
    }
    let action = buf[0];
    // C checks is_skipping_changes() at the entry of the four data-
    // modification handlers only (worker.c:2404,2564,2769,3257); RELATION/
    // TYPE/MESSAGE are processed even while skipping.
    if matches!(action, MSG_INSERT | MSG_UPDATE | MSG_DELETE | MSG_TRUNCATE)
        && crate::is_skipping_changes()
    {
        return Ok(());
    }
    if matches!(
        action,
        MSG_RELATION | MSG_TYPE | MSG_INSERT | MSG_UPDATE | MSG_DELETE | MSG_TRUNCATE | MSG_MESSAGE
    ) && crate::stream_apply::handle_streamed_transaction(action, buf)?
    {
        return Ok(());
    }
    let mut r = Reader::new(&buf[1..]);
    match action {
        MSG_BEGIN => apply_handle_begin(&mut r),
        MSG_COMMIT => apply_handle_commit(mcx, conn, &mut r),
        MSG_ORIGIN => {
            // ORIGIN inside a remote transaction; contents unused here.
            let _ = logicalproto::logicalrep_read_origin(&mut r)?;
            Ok(())
        }
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
        MSG_STREAM_START => crate::stream_apply::apply_handle_stream_start(mcx, &mut r),
        MSG_STREAM_STOP => crate::stream_apply::apply_handle_stream_stop(mcx),
        MSG_STREAM_COMMIT => crate::stream_apply::apply_handle_stream_commit(mcx, conn, &mut r),
        MSG_STREAM_ABORT => crate::stream_apply::apply_handle_stream_abort(mcx, &mut r),
        MSG_STREAM_PREPARE => crate::stream_apply::apply_handle_stream_prepare(mcx, conn, &mut r),
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

// apply_handle_begin (worker.c:985).
fn apply_handle_begin(r: &mut Reader<'_>) -> PgResult<()> {
    let begin = logicalproto::logicalrep_read_begin(r)?;
    REMOTE_FINAL_LSN.set(begin.final_lsn);
    crate::maybe_start_skipping_changes(begin.final_lsn);
    IN_REMOTE_TRANSACTION.set(true);
    Ok(())
}

// apply_handle_commit (worker.c:1010).
fn apply_handle_commit(mcx: Mcx<'static>, conn: &mut PgConn, r: &mut Reader<'_>) -> PgResult<()> {
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
    REMOTE_FINAL_LSN.set(begin.prepare_lsn);
    crate::maybe_start_skipping_changes(begin.prepare_lsn);
    IN_REMOTE_TRANSACTION.set(true);
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
fn apply_handle_prepare(mcx: Mcx<'static>, conn: &mut PgConn, r: &mut Reader<'_>) -> PgResult<()> {
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
    Ok(())
}

// apply_handle_commit_prepared (worker.c:1173).
fn apply_handle_commit_prepared(
    mcx: Mcx<'static>,
    conn: &mut PgConn,
    r: &mut Reader<'_>,
) -> PgResult<()> {
    let prepare_data = logicalproto::logicalrep_read_commit_prepared(r)?;

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

    let local_end = transam_xlog_seams::xact_last_commit_end::call();
    crate::store_flush_position(prepare_data.end_lsn, local_end);
    IN_REMOTE_TRANSACTION.set(false);

    crate::tablesync::process_syncing_tables(mcx, conn, prepare_data.end_lsn)?;

    crate::clear_subscription_skip_lsn(mcx, prepare_data.end_lsn)?;
    Ok(())
}

// apply_handle_rollback_prepared (worker.c:1222).
fn apply_handle_rollback_prepared(
    mcx: Mcx<'static>,
    conn: &mut PgConn,
    r: &mut Reader<'_>,
) -> PgResult<()> {
    let rollback_data = logicalproto::logicalrep_read_rollback_prepared(r)?;

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

        crate::clear_subscription_skip_lsn(mcx, rollback_data.rollback_end_lsn)?;
    }

    // The rollback WAL record is always flushed (worker.c:1259).
    crate::store_flush_position(
        rollback_data.rollback_end_lsn,
        types_core::InvalidXLogRecPtr,
    );
    IN_REMOTE_TRANSACTION.set(false);

    crate::tablesync::process_syncing_tables(mcx, conn, rollback_data.rollback_end_lsn)
}

// apply_handle_relation (worker.c:2318).
fn apply_handle_relation(mcx: Mcx<'_>, r: &mut Reader<'_>) -> PgResult<()> {
    begin_replication_step(mcx)?;
    let rel = logicalproto::logicalrep_read_rel(r)?;
    logicalrelation::logicalrep_relmap_update(&rel);
    end_replication_step()
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
            debug_assert!(m < tup.ncols);
            let bytes = tup.colvalues[m].as_deref().unwrap_or(&[]);
            slot_store_datum(
                mcx,
                infuncs,
                i,
                att.atttypid,
                att.atttypmod,
                tup.colstatus[m],
                bytes,
                m,
            )?
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

        let mut slots = execexpr::EvalSlots { scan: None, inner: None, outer: None };
        let r = execexpr::exec_eval_expr(&mut state, &mut slots)?;
        let base = slot.base_mut();
        base.tts_values[i] = r.value;
        base.tts_isnull[i] = r.isnull;

        keep_alive.push(state);
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
        if tup.colstatus[m] == LOGICALREP_COLUMN_UNCHANGED {
            continue;
        }
        let bytes = tup.colvalues[m].as_deref().unwrap_or(&[]);
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
fn get_tuple_transaction_info(
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

// errdetail_apply_conflict's origin-differs explanation (conflict.c),
// rendered into this file's single-line conflict LOG convention (C's DETAIL
// sentence, lowercased, without the trailing period).
pub(crate) fn origin_differs_detail(
    action: &str,
    localorigin_valid: bool,
    origin_name: Option<&str>,
    localxmin: types_core::TransactionId,
    ts: &str,
) -> String {
    match (localorigin_valid, origin_name) {
        (false, _) => format!(
            "{action} the row that was modified locally in transaction {localxmin} at {ts}"
        ),
        (true, Some(name)) => format!(
            "{action} the row that was modified by a different origin \"{name}\" in transaction {localxmin} at {ts}"
        ),
        // The origin that modified this row has been removed.
        (true, None) => format!(
            "{action} the row that was modified by a non-existent origin in transaction {localxmin} at {ts}"
        ),
    }
}

// ReportApplyConflict for CT_UPDATE_ORIGIN_DIFFERS / CT_DELETE_ORIGIN_DIFFERS
// (conflict.c; called from worker.c:2696,2879), rendered as this file's
// conflict LOG lines.
fn report_origin_differs_conflict(
    mcx: Mcx<'_>,
    entry: &LogicalRepRelMapEntry,
    updating: bool,
    localxmin: types_core::TransactionId,
    localorigin: types_core::RepOriginId,
    localts: types_core::TimestampTz,
) -> PgResult<()> {
    let localorigin_valid = localorigin != types_core::InvalidRepOriginId;
    let origin_name = if localorigin_valid {
        origin::replorigin_by_oid(mcx, localorigin, true)?
    } else {
        None
    };
    let ts = adt_timestamp::timestamptz_to_str(localts);
    let detail = origin_differs_detail(
        if updating { "updating" } else { "deleting" },
        localorigin_valid,
        origin_name.as_deref(),
        localxmin,
        &ts,
    );
    let conflict = if updating { "update_origin_differs" } else { "delete_origin_differs" };
    let (nsp, name) = (&entry.remoterel.nspname, &entry.remoterel.relname);
    let _ = elog::elog(
        LOG,
        format!("conflict detected on relation \"{nsp}.{name}\": conflict={conflict}; {detail}"),
    );
    Ok(())
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

// Refuse row triggers loudly: ExecBR/AR*Triggers are not wired here.
fn refuse_row_triggers(rel: &Relation<'_>, op: &str) {
    if let Some(td) = rel.rd_trigdesc.borrow().as_deref() {
        if td.trig_insert_before_row
            || td.trig_insert_after_row
            || td.trig_update_before_row
            || td.trig_update_after_row
            || td.trig_delete_before_row
            || td.trig_delete_after_row
        {
            panic!("unported: row triggers on logical replication target ({op})");
        }
    }
}

// check_relation_updatable (worker.c:2510).
fn check_relation_updatable(entry: &LogicalRepRelMapEntry) -> PgResult<()> {
    if entry.updatable {
        return Ok(());
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
fn tuples_equal(
    slot1: &mut SlotData<'_>,
    slot2: &mut SlotData<'_>,
    rel: &Relation<'_>,
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
            return Err(Box::new(types_error::PgError::error(format!(
                "could not identify an equality operator for type {}",
                att.atttypid
            ))));
        }
        let eq = fmgr::fcinfo::function_call2_coll(&mut finfo, att.attcollation, v1, v2)?;
        if !eq.as_bool() {
            return Ok(false);
        }
    }
    Ok(true)
}

// build_replindex_scan_key (execReplication.c:55), type-cache-equality
// rendering (see header).
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
            // Expression index keys are not supported in the scan key.
            continue;
        }
        let att = rel.rd_att.attr((table_attno - 1) as usize);
        let typentry =
            typcache::lookup_type_cache(att.atttypid, typcache::TYPECACHE_EQ_OPR_FINFO)?;
        let eq_finfo = typentry.eq_opr_finfo().clone();
        if eq_finfo.fn_oid == InvalidOid {
            return Err(Box::new(types_error::PgError::error(format!(
                "missing equality operator for type {}",
                att.atttypid
            ))));
        }
        let mut key = ScanKeyData::empty();
        key.sk_attno = (i + 1) as i16;
        key.sk_strategy = BTEqualStrategyNumber;
        key.sk_func = eq_finfo;
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

// RelationFindReplTupleByIndex (execReplication.c:179): latest-snapshot +
// tuple-lock-retry rendering of the DirtySnapshot protocol.
fn find_repl_tuple_by_index<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    idxoid: Oid,
    searchslot: &mut SlotData<'mcx>,
    outslot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    let idxrel = indexam::index_open(mcx, idxoid, types_rel::RowExclusiveLock)?;
    let is_safe_skip_dup = true; // idxoid is always the replident/PK here
    let _ = is_safe_skip_dup;

    let keys = build_replindex_scan_key(&idxrel, rel, searchslot)?;

    let found = loop {
        // The scan snapshot must be registered (heapam visibility asserts
        // regd_count/active_count; C's index_beginscan requires the same).
        let snap = snapmgr::RegisterSnapshot(Some(&snapmgr::GetLatestSnapshot()?))?
            .expect("registered snapshot");
        let mut scan =
            indexam::index_beginscan(mcx, rel, &idxrel, snap.clone(), keys.len() as i32, 0)?;
        let mut kv = mcx::PgVec::new_in(mcx);
        for k in &keys {
            kv.push(k.clone());
        }
        indexam::index_rescan(&mut scan, Some(&kv), None)?;

        let mut found = false;
        while indexam::index_getnext_slot(
            mcx,
            &mut scan,
            types_scan::sdir::ScanDirection::ForwardScanDirection,
            outslot,
        )? {
            // ExecMaterializeSlot (execReplication.c:228): own the tuple
            // before the scan's pin goes away.
            exectuples::exec_materialize_slot(outslot, mcx)?;
            found = true;
            break;
        }

        if found {
            // Lock the found tuple; on concurrent update/delete retry the scan
            // (should_refetch_tuple protocol).
            let tid = outslot.base().tts_tid;
            snapmgr::PushActiveSnapshot(&snapmgr::GetLatestSnapshot()?)?;
            let lockres = tableam::table_tuple_lock_for_repl(mcx, rel, &tid, outslot);
            snapmgr::PopActiveSnapshot()?;
            match lockres? {
                LockOutcome::Ok => {
                    indexam::index_endscan(scan)?;
                    snapmgr::UnregisterSnapshot(Some(&snap));
                    break true;
                }
                LockOutcome::Retry => {
                    indexam::index_endscan(scan)?;
                    snapmgr::UnregisterSnapshot(Some(&snap));
                    continue;
                }
            }
        }
        indexam::index_endscan(scan)?;
        snapmgr::UnregisterSnapshot(Some(&snap));
        break false;
    };

    indexam::index_close(idxrel, types_rel::NoLock)?;
    Ok(found)
}

enum LockOutcome {
    Ok,
    Retry,
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
        Ok(match res {
            TM_Result::TM_Ok => LockOutcome::Ok,
            TM_Result::TM_Updated | TM_Result::TM_Deleted => LockOutcome::Retry,
            TM_Result::TM_SelfModified => LockOutcome::Ok,
            other => {
                return Err(Box::new(types_error::PgError::error(format!(
                    "unexpected table_tuple_lock status {}",
                    other as u32
                ))))
            }
        })
    }
}

// RelationFindReplTupleSeq (execReplication.c:355).
fn find_repl_tuple_seq<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    searchslot: &mut SlotData<'mcx>,
    outslot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    let found = loop {
        // Registered for the scan's lifetime (see the by-index variant).
        let snap = snapmgr::RegisterSnapshot(Some(&snapmgr::GetLatestSnapshot()?))?
            .expect("registered snapshot");
        let mut scan =
            tableam_real::table_beginscan(mcx, rel, Some(snap.clone()), 0, mcx::PgVec::new_in(mcx))?;
        let mut scanslot = tableam_real::table_slot_create(mcx, rel)?;

        let mut found = false;
        while tableam_real::table_scan_getnextslot(
            mcx,
            &mut scan,
            types_scan::sdir::ScanDirection::ForwardScanDirection,
            &mut scanslot,
        )? {
            if !tuples_equal(&mut scanslot, searchslot, rel)? {
                continue;
            }
            found = true;
            break;
        }

        if found {
            // Copy the match out, then lock it (retry on concurrent change).
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
            // ExecCopySlot is a DEEP copy in C: materialize now, while the
            // datums still point into scanslot's pinned buffer.
            exectuples::exec_materialize_slot(outslot, mcx)?;

            let tid = outslot.base().tts_tid;
            snapmgr::PushActiveSnapshot(&snapmgr::GetLatestSnapshot()?)?;
            let lockres = tableam::table_tuple_lock_for_repl(mcx, rel, &tid, outslot);
            snapmgr::PopActiveSnapshot()?;
            tableam_real::table_endscan(scan)?;
            snapmgr::UnregisterSnapshot(Some(&snap));
            match lockres? {
                LockOutcome::Ok => break true,
                LockOutcome::Retry => continue,
            }
        }
        tableam_real::table_endscan(scan)?;
        snapmgr::UnregisterSnapshot(Some(&snap));
        break false;
    };
    Ok(found)
}

// FindReplTupleInLocalRel (worker.c:2915).
fn find_repl_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    entry: &LogicalRepRelMapEntry,
    searchslot: &mut SlotData<'mcx>,
    outslot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
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
    refuse_row_triggers(rel, "INSERT");
    execreplication::CheckCmdReplicaIdentity(mcx, rel, types_nodes::nodes_enums::CmdType::CMD_INSERT)?;

    let mut generated_exprs = None;
    if rel.rd_att.constr.as_deref().is_some_and(|c| c.has_generated_stored) {
        nodemodifytable::exec_compute_stored_generated(mcx, &mut generated_exprs, rel, slot)?;
    }
    let mut check_exprs = None;
    let mut nn_exprs = None;
    if rel.rd_att.constr.is_some() {
        nodemodifytable::exec_constraints(mcx, &mut check_exprs, &mut nn_exprs, rel, slot, None, None)?;
    }

    tableam_real::simple_table_tuple_insert(mcx, rel, slot)?;

    let mut index_state = execindexing::ExecOpenIndices(mcx, rel, false)?;
    if index_state.num_indices() > 0 {
        let eval_cx = mcx::MemoryContext::new("ApplyIndexEval");
        execindexing::ExecInsertIndexTuples(
            mcx,
            eval_cx.mcx(),
            &mut index_state,
            rel,
            slot,
            false,
            None,
            &[],
            false,
        )?;
    }
    execindexing::ExecCloseIndices(index_state)?;
    Ok(())
}

// ExecSimpleRelationUpdate rendering (execReplication.c:651).
fn do_update<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    searchslot: &mut SlotData<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<()> {
    use tableam_vocab::TU_UpdateIndexes;

    refuse_row_triggers(rel, "UPDATE");
    execreplication::CheckCmdReplicaIdentity(mcx, rel, types_nodes::nodes_enums::CmdType::CMD_UPDATE)?;

    let mut generated_exprs = None;
    if rel.rd_att.constr.as_deref().is_some_and(|c| c.has_generated_stored) {
        nodemodifytable::exec_compute_stored_generated(mcx, &mut generated_exprs, rel, slot)?;
    }
    let mut check_exprs = None;
    let mut nn_exprs = None;
    if rel.rd_att.constr.is_some() {
        nodemodifytable::exec_constraints(mcx, &mut check_exprs, &mut nn_exprs, rel, slot, None, None)?;
    }

    let otid = searchslot.base().tts_tid;
    let snap = Some(snapmgr::GetActiveSnapshot());
    let mut update_indexes = TU_UpdateIndexes::TU_None;
    tableam_real::simple_table_tuple_update(mcx, rel, &otid, slot, &snap, &mut update_indexes)?;

    if !matches!(update_indexes, TU_UpdateIndexes::TU_None) {
        let only_summarizing = matches!(update_indexes, TU_UpdateIndexes::TU_Summarizing);
        let mut index_state = execindexing::ExecOpenIndices(mcx, rel, false)?;
        if index_state.num_indices() > 0 {
            let eval_cx = mcx::MemoryContext::new("ApplyIndexEval");
            execindexing::ExecInsertIndexTuples(
                mcx,
                eval_cx.mcx(),
                &mut index_state,
                rel,
                slot,
                false,
                None,
                &[],
                only_summarizing,
            )?;
        }
        execindexing::ExecCloseIndices(index_state)?;
    }
    Ok(())
}

// ExecSimpleRelationDelete rendering (execReplication.c:734).
fn do_delete<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    searchslot: &mut SlotData<'mcx>,
) -> PgResult<()> {
    refuse_row_triggers(rel, "DELETE");
    execreplication::CheckCmdReplicaIdentity(mcx, rel, types_nodes::nodes_enums::CmdType::CMD_DELETE)?;
    let tid = searchslot.base().tts_tid;
    let snap = Some(snapmgr::GetActiveSnapshot());
    tableam_real::simple_table_tuple_delete(mcx, rel, &tid, &snap)
}

// apply_handle_insert (worker.c:2388).
fn apply_handle_insert(mcx: Mcx<'static>, r: &mut Reader<'_>) -> PgResult<()> {
    begin_replication_step(mcx)?;

    let (relid, newtup) = logicalproto::logicalrep_read_insert(r)?;
    let subid = my_sub(|s| s.oid);
    let (entry, rel) =
        logicalrelation::logicalrep_rel_open(mcx, relid, types_rel::RowExclusiveLock, subid)?;
    if !should_apply_changes_for_rel(&entry) {
        logicalrelation::logicalrep_rel_close(rel, types_rel::RowExclusiveLock)?;
        return end_replication_step();
    }

    // Make sure that any user-supplied code runs as the table owner, unless
    // the user has opted out of that behavior (worker.c:2427).
    let ucxt = maybe_switch_to_table_owner(mcx, rel.rd_rel.relowner)?;

    // Keep the input-function cache alive past do_insert: flinfo-owned
    // scratch backs the by-ref datums in the slot (see InFuncs).
    let mut infuncs = InFuncs::new(rel.rd_att.natts as usize);
    let mut remoteslot = tableam_real::table_slot_create(mcx, &rel)?;
    slot_store_data(mcx, &mut remoteslot, &entry, &rel, &newtup, &mut infuncs)?;
    // Keep-alive for the same reason as infuncs: default datums may live in
    // flinfo-owned scratch until do_insert materializes the tuple.
    let _default_exprs = slot_fill_defaults(mcx, &entry, &rel, &mut remoteslot)?;

    do_insert(mcx, &rel, &mut remoteslot)?;

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
    if !should_apply_changes_for_rel(&entry) {
        logicalrelation::logicalrep_rel_close(rel, types_rel::RowExclusiveLock)?;
        return end_replication_step();
    }
    check_relation_updatable(&entry)?;

    // Make sure that any user-supplied code runs as the table owner, unless
    // the user has opted out of that behavior (worker.c:2594).
    let ucxt = maybe_switch_to_table_owner(mcx, rel.rd_rel.relowner)?;

    let mut infuncs = InFuncs::new(rel.rd_att.natts as usize);
    let mut remoteslot = tableam_real::table_slot_create(mcx, &rel)?;
    let searchtup = if upd.has_oldtuple {
        upd.oldtup.as_ref().expect("oldtuple present")
    } else {
        &upd.newtup
    };
    slot_store_data(mcx, &mut remoteslot, &entry, &rel, searchtup, &mut infuncs)?;

    let mut localslot = tableam_real::table_slot_create(mcx, &rel)?;
    let found = find_repl_tuple(mcx, &rel, &entry, &mut remoteslot, &mut localslot)?;

    if found {
        // Report the conflict if the tuple was modified by a different origin
        // (worker.c:2696): commit-ts data present and origin != session's.
        let (localxmin, ctsdata) = get_tuple_transaction_info(&localslot)?;
        if let Some((localts, localorigin)) = ctsdata {
            if localorigin != origin::replorigin_session_origin() {
                report_origin_differs_conflict(mcx, &entry, true, localxmin, localorigin, localts)?;
            }
        }

        // A second cache: slot_modify_data's writes reuse per-column scratch,
        // which would invalidate remoteslot's datums mid-use otherwise.
        let mut modfuncs = InFuncs::new(rel.rd_att.natts as usize);
        let mut newslot = tableam_real::table_slot_create(mcx, &rel)?;
        slot_modify_data(mcx, &mut newslot, &mut localslot, &entry, &rel, &upd.newtup, &mut modfuncs)?;
        do_update(mcx, &rel, &mut localslot, &mut newslot)?;
    } else {
        let (nsp, name) =
            (&entry.remoterel.nspname, &entry.remoterel.relname);
        let _ = elog::elog(
            LOG,
            format!(
                "conflict detected on relation \"{nsp}.{name}\": conflict=update_missing; \
                 could not find the row to be updated"
            ),
        );
    }

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
    if !should_apply_changes_for_rel(&entry) {
        logicalrelation::logicalrep_rel_close(rel, types_rel::RowExclusiveLock)?;
        return end_replication_step();
    }
    check_relation_updatable(&entry)?;

    // Make sure that any user-supplied code runs as the table owner, unless
    // the user has opted out of that behavior (worker.c:2798).
    let ucxt = maybe_switch_to_table_owner(mcx, rel.rd_rel.relowner)?;

    let mut infuncs = InFuncs::new(rel.rd_att.natts as usize);
    let mut remoteslot = tableam_real::table_slot_create(mcx, &rel)?;
    slot_store_data(mcx, &mut remoteslot, &entry, &rel, &oldtup, &mut infuncs)?;

    let mut localslot = tableam_real::table_slot_create(mcx, &rel)?;
    let found = find_repl_tuple(mcx, &rel, &entry, &mut remoteslot, &mut localslot)?;

    if found {
        // Report the conflict if the tuple was modified by a different origin
        // (worker.c:2879).
        let (localxmin, ctsdata) = get_tuple_transaction_info(&localslot)?;
        if let Some((localts, localorigin)) = ctsdata {
            if localorigin != origin::replorigin_session_origin() {
                report_origin_differs_conflict(mcx, &entry, false, localxmin, localorigin, localts)?;
            }
        }

        do_delete(mcx, &rel, &mut localslot)?;
    } else {
        let (nsp, name) =
            (&entry.remoterel.nspname, &entry.remoterel.relname);
        let _ = elog::elog(
            LOG,
            format!(
                "conflict detected on relation \"{nsp}.{name}\": conflict=delete_missing; \
                 could not find the row to be deleted"
            ),
        );
    }

    restore_user_context(&ucxt)?;

    logicalrelation::logicalrep_rel_close(rel, types_rel::NoLock)?;
    end_replication_step()
}

// apply_handle_truncate (worker.c:3232). Even if the publisher used CASCADE,
// C explicitly replays without further cascading (DROP_RESTRICT).
// TargetPrivilegesCheck (ACL_TRUNCATE) is elided as in the other handlers
// (recorded divergence); C's truncate has no run-as-owner arm.
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
        if !should_apply_changes_for_rel(&entry) {
            logicalrelation::logicalrep_rel_close(rel, types_rel::AccessExclusiveLock)?;
            continue;
        }
        if rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
            // C fans the truncate out to the leaf partitions
            // (find_all_inheritors); partitioned targets are unported in this
            // apply worker across all handlers.
            panic!("unported: TRUNCATE apply on a partitioned table");
        }
        if heapam::relation_is_logically_logged(&rel) {
            relids_logged.push(rel.rd_id);
        }
        relids.push(rel.rd_id);
        rels.push(rel);
    }

    if !rels.is_empty() {
        tablecmds::ExecuteTruncateGuts(
            mcx,
            &mut rels,
            &mut relids,
            &mut relids_logged,
            types_nodes::parsenodes::DropBehavior::DROP_RESTRICT,
            restart_seqs,
        )?;
    }

    for rel in rels {
        logicalrelation::logicalrep_rel_close(rel, types_rel::NoLock)?;
    }

    end_replication_step()
}
