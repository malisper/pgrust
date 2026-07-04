use std::sync::atomic::Ordering::Relaxed;

use elog::ereport;
use lwlock::LW_EXCLUSIVE;
use types_core::xact::XlXactStatsItem;
use types_core::{Oid, TimestampTz, TransactionId, XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK};
use types_error::{PgResult, ERRCODE_DATA_CORRUPTED, ERROR};
use types_storage::{RelFileLocator, SharedInvalidationMessage};

use crate::codec::{
    BufferLayout, TwoPhaseFileHeader, SIZEOF_REL_FILE_LOCATOR, SIZEOF_SHARED_INVAL_MSG,
    SIZEOF_XL_XACT_STATS_ITEM,
};
use crate::core::{
    corrupt_guard, lock_gxact, process_records, remove_gxact, xlog_read_twophase_data,
    DO_NOT_REPLICATE_ID,
};
use crate::files;
use crate::here;
use crate::state::{lock_twophase_state, unlock_twophase_state, TwoPhaseState, MY_LOCKED_GXACT, NO_GXACT};

fn decode_rels(buf: &[u8], base: usize, n: usize) -> Vec<RelFileLocator> {
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        let o = base + i * SIZEOF_REL_FILE_LOCATOR;
        let u = |k: usize| u32::from_ne_bytes(buf[o + k..o + k + 4].try_into().unwrap());
        v.push(RelFileLocator {
            spcOid: u(0),
            dbOid: u(4),
            relNumber: u(8),
        });
    }
    v
}

fn decode_stats(buf: &[u8], base: usize, n: usize) -> Vec<XlXactStatsItem> {
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        let o = base + i * SIZEOF_XL_XACT_STATS_ITEM;
        let kind = i32::from_ne_bytes(buf[o..o + 4].try_into().unwrap());
        let dboid = u32::from_ne_bytes(buf[o + 4..o + 8].try_into().unwrap());
        let lo = u32::from_ne_bytes(buf[o + 8..o + 12].try_into().unwrap());
        let hi = u32::from_ne_bytes(buf[o + 12..o + 16].try_into().unwrap());
        v.push(XlXactStatsItem {
            kind,
            dboid,
            objid: ((hi as u64) << 32) | lo as u64,
        });
    }
    v
}

fn decode_invals(buf: &[u8], base: usize, n: usize) -> PgResult<Vec<SharedInvalidationMessage>> {
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        let o = base + i * SIZEOF_SHARED_INVAL_MSG;
        let raw: [u8; SIZEOF_SHARED_INVAL_MSG] =
            buf[o..o + SIZEOF_SHARED_INVAL_MSG].try_into().unwrap();
        let msg = SharedInvalidationMessage::from_wire_bytes(raw).ok_or_else(|| {
            ereport(ERROR)
                .errcode(ERRCODE_DATA_CORRUPTED)
                .errmsg("invalid shared invalidation message in two-phase state")
                .finish(here("FinishPreparedTransaction"))
                .unwrap_err()
        })?;
        v.push(msg);
    }
    Ok(v)
}

/// `FinishPreparedTransaction(gid, isCommit)` — COMMIT/ROLLBACK PREPARED.
pub fn FinishPreparedTransaction(gid: &str, is_commit: bool) -> PgResult<()> {
    let idx = lock_gxact(gid, miscinit::GetUserId())?;
    let st = TwoPhaseState();
    let g = st.gxact(idx);
    let pgprocno = g.pgprocno.get();
    let xid = g.xid.get();

    let buf = if g.ondisk.get() {
        files::read_twophase_file(xid, false)?.expect("two-phase state file disappeared")
    } else {
        xlog_read_twophase_data(g.prepare_start_lsn.get())?
    };

    let hdr = corrupt_guard(TwoPhaseFileHeader::from_bytes(&buf), "FinishPreparedTransaction")?;
    debug_assert_eq!(hdr.xid, xid);
    let layout = BufferLayout::of(&hdr);
    let children: Vec<TransactionId> = {
        let mut v = Vec::with_capacity(hdr.nsubxacts as usize);
        for i in 0..hdr.nsubxacts as usize {
            let o = layout.children + i * 4;
            v.push(TransactionId::from_ne_bytes(buf[o..o + 4].try_into().unwrap()));
        }
        v
    };
    let commitrels = decode_rels(&buf, layout.commitrels, hdr.ncommitrels as usize);
    let abortrels = decode_rels(&buf, layout.abortrels, hdr.nabortrels as usize);
    let commitstats = decode_stats(&buf, layout.commitstats, hdr.ncommitstats as usize);
    let abortstats = decode_stats(&buf, layout.abortstats, hdr.nabortstats as usize);
    let invalmsgs = decode_invals(&buf, layout.invalmsgs, hdr.ninvalmsgs as usize)?;

    let latest_xid = transam::TransactionIdLatest(xid, &children);

    init_small::globals::HoldInterrupts();

    // Order is critical: WAL record, then pg_xact, then ProcArray removal,
    // then the post-commit/post-abort callbacks (which release the locks).
    if is_commit {
        record_transaction_commit_prepared(
            xid,
            &children,
            &commitrels,
            &commitstats,
            &invalmsgs,
            hdr.initfileinval,
            gid,
        )?;
    } else {
        record_transaction_abort_prepared(xid, &children, &abortrels, &abortstats, gid)?;
    }

    procarray::ProcArrayRemove(pgprocno, latest_xid)?;

    // If the callbacks fail, the gxact must not look committable again; it is
    // still locked by us so it can't be recycled underneath us. C does this
    // unlocked too.
    g.valid.set(false);

    let delrels = if is_commit { &commitrels } else { &abortrels };
    catalog_storage::DropRelationFiles(delrels, false)?;

    if is_commit {
        pgstat::xact::pgstat_execute_transactional_drops(&commitstats, false)?;
    } else {
        pgstat::xact::pgstat_execute_transactional_drops(&abortstats, false)?;
    }

    if is_commit {
        if hdr.initfileinval {
            relcache_seams::relation_cache_init_file_pre_invalidate::call()?;
        }
        sinval::SendSharedInvalidMessages(&invalmsgs)?;
        if hdr.initfileinval {
            relcache_seams::relation_cache_init_file_post_invalidate::call()?;
        }
    }

    // Hold TwoPhaseStateLock across the callbacks so a concurrent reuse of the
    // same GID can't collide; release after the shared state is cleared.
    lock_twophase_state(LW_EXCLUSIVE);

    let callbacks = if is_commit {
        &twophase_rmgr::twophase_postcommit_callbacks
    } else {
        &twophase_rmgr::twophase_postabort_callbacks
    };
    let cb_result = process_records(&buf, layout.records, xid, callbacks)
        .and_then(|()| predicate::PredicateLockTwoPhaseFinish(xid, is_commit));

    let ondisk = g.ondisk.get();
    remove_gxact(idx);

    unlock_twophase_state();
    cb_result?;

    pgstat::xact::AtEOXact_PgStat(is_commit, false);

    if ondisk {
        files::remove_two_phase_file(xid, true)?;
    }

    MY_LOCKED_GXACT.set(NO_GXACT);

    init_small::globals::ResumeInterrupts();
    Ok(())
}

fn replorigin_session() -> (types_core::RepOriginId, u64, TimestampTz) {
    if origin_seams::replorigin_session_origin::is_installed() {
        (
            origin_seams::replorigin_session_origin::call(),
            origin_seams::replorigin_session_origin_lsn::call(),
            origin_seams::replorigin_session_origin_timestamp::call(),
        )
    } else {
        (0, 0, 0)
    }
}

/// `RecordTransactionCommitPrepared` — mirrors RecordTransactionCommit's
/// DELAY_CHKPT_START discipline.
fn record_transaction_commit_prepared(
    xid: TransactionId,
    children: &[TransactionId],
    rels: &[RelFileLocator],
    stats: &[XlXactStatsItem],
    invalmsgs: &[SharedInvalidationMessage],
    initfileinval: bool,
    gid: &str,
) -> PgResult<()> {
    let committs = timestamp_seams::get_current_timestamp::call();
    let (origin, origin_lsn, mut origin_ts) = replorigin_session();
    let replorigin = origin != 0 && origin != DO_NOT_REPLICATE_ID;

    let my_proc = lmgr_proc::GetPGProcByNumber(init_small::globals::MyProcNumber());
    init_small::globals::StartCriticalSection();
    debug_assert_eq!(
        my_proc.delayChkptFlags.load(Relaxed) & types_storage::storage::DELAY_CHKPT_START,
        0
    );
    my_proc
        .delayChkptFlags
        .fetch_or(types_storage::storage::DELAY_CHKPT_START, Relaxed);

    // 2PC commits are marked as potentially having AccessExclusiveLocks.
    let recptr = xact::XactLogCommitRecord(
        committs,
        children,
        rels,
        stats,
        invalmsgs,
        initfileinval,
        xact::MyXactFlags() | XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK,
        xid,
        Some(gid),
    )?;

    if replorigin {
        origin_seams::replorigin_session_advance::call(
            origin_lsn,
            xlog_seams::xact_last_rec_end::call(),
        )?;
    }

    if !replorigin || origin_ts == 0 {
        origin_ts = committs;
        if origin_seams::set_replorigin_session_origin_timestamp::is_installed() {
            origin_seams::set_replorigin_session_origin_timestamp::call(origin_ts);
        }
    }

    if commit_ts_seams::transaction_tree_set_commit_ts_data::is_installed() {
        commit_ts_seams::transaction_tree_set_commit_ts_data::call(xid, children, origin_ts, origin)?;
    }

    transam_xlog::XLogFlush(recptr)?;

    transam::TransactionIdCommitTree(xid, children)?;

    my_proc
        .delayChkptFlags
        .fetch_and(!types_storage::storage::DELAY_CHKPT_START, Relaxed);
    init_small::globals::EndCriticalSection();

    if syncrep_seams::sync_rep_wait_for_lsn::is_installed() {
        syncrep_seams::sync_rep_wait_for_lsn::call(recptr, true)?;
    }
    Ok(())
}

/// `RecordTransactionAbortPrepared`.
fn record_transaction_abort_prepared(
    xid: TransactionId,
    children: &[TransactionId],
    rels: &[RelFileLocator],
    stats: &[XlXactStatsItem],
    gid: &str,
) -> PgResult<()> {
    let (origin, origin_lsn, _) = replorigin_session();
    let replorigin = origin != 0 && origin != DO_NOT_REPLICATE_ID;

    // Catch an abort partway through RecordTransactionCommitPrepared.
    if transam::TransactionIdDidCommit(xid)? {
        panic!("cannot abort transaction {xid}, it was already committed");
    }

    init_small::globals::StartCriticalSection();

    let recptr = xact::XactLogAbortRecord(
        timestamp_seams::get_current_timestamp::call(),
        children,
        rels,
        stats,
        xact::MyXactFlags() | XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK,
        xid,
        Some(gid),
    )?;

    if replorigin {
        origin_seams::replorigin_session_advance::call(
            origin_lsn,
            xlog_seams::xact_last_rec_end::call(),
        )?;
    }

    // Always flush: we're about to remove the 2PC state file.
    transam_xlog::XLogFlush(recptr)?;

    transam::TransactionIdAbortTree(xid, children)?;

    init_small::globals::EndCriticalSection();

    if syncrep_seams::sync_rep_wait_for_lsn::is_installed() {
        syncrep_seams::sync_rep_wait_for_lsn::call(recptr, false)?;
    }
    Ok(())
}

/// `LookupGXact` — logical-replication duplicate detection.
pub fn LookupGXact(
    gid: &str,
    prepare_end_lsn: u64,
    origin_prepare_timestamp: TimestampTz,
) -> PgResult<bool> {
    let st = TwoPhaseState();
    let mut found = false;
    lock_twophase_state(lwlock::LW_SHARED);
    let inner = (|| -> PgResult<()> {
        for i in 0..st.num_prep_xacts.get() {
            let g = st.gxact(st.prep_xact(i));
            if !g.valid.get() || g.gid.get().as_str() != gid {
                continue;
            }
            let buf = if g.ondisk.get() {
                files::read_twophase_file(g.xid.get(), false)?
                    .expect("two-phase state file disappeared")
            } else {
                debug_assert!(g.prepare_start_lsn.get() != 0);
                xlog_read_twophase_data(g.prepare_start_lsn.get())?
            };
            let hdr = corrupt_guard(TwoPhaseFileHeader::from_bytes(&buf), "LookupGXact")?;
            if hdr.origin_lsn == prepare_end_lsn
                && hdr.origin_timestamp == origin_prepare_timestamp
            {
                found = true;
                break;
            }
        }
        Ok(())
    })();
    unlock_twophase_state();
    inner?;
    Ok(found)
}

/// One `pg_prepared_xacts` row.
pub struct PreparedXactRow {
    pub transaction: TransactionId,
    pub gid: String,
    pub prepared: TimestampTz,
    pub ownerid: Oid,
    pub dbid: Oid,
}

/// `GetPreparedTransactionList` + the pg_prepared_xact row projection (valid
/// entries only; the SRF frame lives in srf.rs).
pub(crate) fn prepared_xact_rows() -> Vec<PreparedXactRow> {
    let st = TwoPhaseState();
    let mut rows = Vec::new();
    lock_twophase_state(lwlock::LW_SHARED);
    for i in 0..st.num_prep_xacts.get() {
        let g = st.gxact(st.prep_xact(i));
        if !g.valid.get() {
            continue;
        }
        let proc = lmgr_proc::GetPGProcByNumber(g.pgprocno.get());
        rows.push(PreparedXactRow {
            transaction: proc.xid.read(),
            gid: g.gid.get().as_str().to_owned(),
            prepared: g.prepared_at.get(),
            ownerid: g.owner.get(),
            dbid: proc.databaseId.load(Relaxed),
        });
    }
    unlock_twophase_state();
    rows
}
