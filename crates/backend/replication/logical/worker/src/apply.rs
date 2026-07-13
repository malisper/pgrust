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
// - TRUNCATE apply refuses loudly (ExecuteTruncateGuts unported here).
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
    PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_PROTOCOL_VIOLATION, ERROR, LOG,
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

// begin_replication_step (worker.c:501).
fn begin_replication_step(mcx: Mcx<'_>) -> PgResult<()> {
    if !xact::IsTransactionState() {
        xact::StartTransactionCommand()?;
        crate::maybe_reread_subscription(mcx)?;
    }
    let snap = snapmgr::GetTransactionSnapshot()?;
    snapmgr::PushActiveSnapshot(&snap)?;
    Ok(())
}

// end_replication_step (worker.c:524).
fn end_replication_step() -> PgResult<()> {
    snapmgr::PopActiveSnapshot()?;
    xact::CommandCounterIncrement()
}

// should_apply_changes_for_rel (worker.c:461), leader-apply arm. Non-READY
// states belong to tablesync (inc E) and refuse loudly rather than desync.
fn should_apply_changes_for_rel(entry: &LogicalRepRelMapEntry) -> bool {
    const SUBREL_STATE_READY: u8 = b'r';
    const SUBREL_STATE_SYNCDONE: u8 = b's';
    const SUBREL_STATE_UNKNOWN: u8 = 0;
    match entry.state {
        SUBREL_STATE_READY => true,
        SUBREL_STATE_SYNCDONE => entry.statelsn <= REMOTE_FINAL_LSN.get(),
        SUBREL_STATE_UNKNOWN => false,
        other => panic!(
            "unported: tablesync relation state '{}' during apply (round-4 inc E)",
            other as char
        ),
    }
}

// apply_dispatch (worker.c:3368).
pub(crate) fn apply_dispatch(mcx: Mcx<'static>, conn: &mut PgConn, buf: &[u8]) -> PgResult<()> {
    if buf.is_empty() {
        return Ok(());
    }
    let action = buf[0];
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
        MSG_TRUNCATE => panic!("unported: TRUNCATE apply (ExecuteTruncateGuts)"),
        MSG_MESSAGE => Ok(()), // transactional messages are ignored by apply
        MSG_STREAM_START | MSG_STREAM_STOP | MSG_STREAM_COMMIT | MSG_STREAM_ABORT
        | MSG_STREAM_PREPARE => {
            panic!("unported: streamed transaction apply (phase-2); message '{}'", action as char)
        }
        MSG_BEGIN_PREPARE | MSG_PREPARE | MSG_COMMIT_PREPARED | MSG_ROLLBACK_PREPARED => {
            panic!("unported: two-phase apply (phase-2); message '{}'", action as char)
        }
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
    IN_REMOTE_TRANSACTION.set(true);
    Ok(())
}

// apply_handle_commit (worker.c:1010) + apply_handle_commit_internal (:2258).
fn apply_handle_commit(mcx: Mcx<'_>, _conn: &mut PgConn, r: &mut Reader<'_>) -> PgResult<()> {
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

    if xact::IsTransactionState() {
        // Update origin state so streaming restarts from the right position
        // after a crash: the session origin lsn/timestamp ride the local
        // commit record (origin crate seams feed xact's commit writer).
        origin::set_replorigin_session_origin_lsn(commit.end_lsn);
        origin::set_replorigin_session_origin_timestamp(commit.committime);

        xact::CommitTransactionCommand()?;

        let local_end = transam_xlog_seams::xact_last_commit_end::call();
        crate::store_flush_position(commit.end_lsn, local_end);
    } else {
        // Empty transaction: process pending invals.
        inval::local::AcceptInvalidationMessages()?;
        crate::maybe_reread_subscription(mcx)?;
    }

    IN_REMOTE_TRANSACTION.set(false);
    // process_syncing_tables: no tablesync workers exist in this subset; a
    // non-READY table would already have refused in should_apply_changes.
    Ok(())
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
) -> PgResult<()> {
    let natts = rel.rd_att.natts as usize;
    exectuples::exec_clear_tuple(slot, mcx);

    for i in 0..natts {
        let att = rel.rd_att.attr(i);
        let remote = entry.attrmap.get(i).copied().unwrap_or(-1);
        let (value, isnull) = if !att.attisdropped && remote >= 0 {
            let m = remote as usize;
            debug_assert!(m < tup.ncols);
            match tup.colstatus[m] {
                LOGICALREP_COLUMN_TEXT => {
                    let (typinput, typioparam) = lsyscache::getTypeInputInfo(att.atttypid)?;
                    let mut flinfo = fmgr_seams::fmgr_info::call(typinput)?;
                    let bytes = tup.colvalues[m].as_deref().unwrap_or(&[]);
                    let cstr = CString::new(bytes).map_err(|_| {
                        Box::new(types_error::PgError::error(
                            "invalid text column data in logical replication message".to_string(),
                        ))
                    })?;
                    let d = fmgr::soft::input_function_call(
                        &mut flinfo,
                        Some(cstr.as_c_str()),
                        typioparam,
                        att.atttypmod,
                        mcx,
                    )?;
                    (d, false)
                }
                LOGICALREP_COLUMN_BINARY => {
                    panic!("unported: binary-format logical replication column (binary=true)")
                }
                _ => (Datum::null(), true), // NULL (or unexpected UNCHANGED)
            }
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

// slot_modify_data (worker.c:892): copy srcslot, replace replicated columns.
fn slot_modify_data<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
    srcslot: &mut SlotData<'mcx>,
    entry: &LogicalRepRelMapEntry,
    rel: &Relation<'mcx>,
    tup: &LogicalRepTupleData,
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
        let (value, isnull) = match tup.colstatus[m] {
            LOGICALREP_COLUMN_TEXT => {
                let (typinput, typioparam) = lsyscache::getTypeInputInfo(att.atttypid)?;
                let mut flinfo = fmgr_seams::fmgr_info::call(typinput)?;
                let bytes = tup.colvalues[m].as_deref().unwrap_or(&[]);
                let cstr = CString::new(bytes).map_err(|_| {
                    Box::new(types_error::PgError::error(
                        "invalid text column data in logical replication message".to_string(),
                    ))
                })?;
                let d = fmgr::soft::input_function_call(
                    &mut flinfo,
                    Some(cstr.as_c_str()),
                    typioparam,
                    att.atttypmod,
                    mcx,
                )?;
                (d, false)
            }
            LOGICALREP_COLUMN_BINARY => {
                panic!("unported: binary-format logical replication column (binary=true)")
            }
            _ => (Datum::null(), true), // LOGICALREP_COLUMN_NULL
        };
        let base = slot.base_mut();
        base.tts_values[i] = value;
        base.tts_isnull[i] = isnull;
    }

    exectuples::exec_store_virtual_tuple(slot);
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
        let snap = snapmgr::GetLatestSnapshot()?;
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
                    break true;
                }
                LockOutcome::Retry => {
                    indexam::index_endscan(scan)?;
                    continue;
                }
            }
        }
        indexam::index_endscan(scan)?;
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
        let snap = snapmgr::GetLatestSnapshot()?;
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

    let mut remoteslot = tableam_real::table_slot_create(mcx, &rel)?;
    slot_store_data(mcx, &mut remoteslot, &entry, &rel, &newtup)?;
    // slot_fill_defaults: subscriber-only columns keep NULL; evaluating
    // non-NULL column defaults on apply is not ported (recorded divergence —
    // C fills defaults for columns absent on the publisher).

    do_insert(mcx, &rel, &mut remoteslot)?;

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

    let mut remoteslot = tableam_real::table_slot_create(mcx, &rel)?;
    let searchtup = if upd.has_oldtuple {
        upd.oldtup.as_ref().expect("oldtuple present")
    } else {
        &upd.newtup
    };
    slot_store_data(mcx, &mut remoteslot, &entry, &rel, searchtup)?;

    let mut localslot = tableam_real::table_slot_create(mcx, &rel)?;
    let found = find_repl_tuple(mcx, &rel, &entry, &mut remoteslot, &mut localslot)?;

    if found {
        let mut newslot = tableam_real::table_slot_create(mcx, &rel)?;
        slot_modify_data(mcx, &mut newslot, &mut localslot, &entry, &rel, &upd.newtup)?;
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

    let mut remoteslot = tableam_real::table_slot_create(mcx, &rel)?;
    slot_store_data(mcx, &mut remoteslot, &entry, &rel, &oldtup)?;

    let mut localslot = tableam_real::table_slot_create(mcx, &rel)?;
    let found = find_repl_tuple(mcx, &rel, &entry, &mut remoteslot, &mut localslot)?;

    if found {
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

    logicalrelation::logicalrep_rel_close(rel, types_rel::NoLock)?;
    end_replication_step()
}
