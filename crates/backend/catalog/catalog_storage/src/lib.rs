// storage.c. pendingDeletes: Vec, index 0 == C list head (C prepends);
// pendingSyncs: Vec where C uses a hash — entries per transaction are the
// relations created under wal_level=minimal, always tiny.
#![allow(non_snake_case)]

use std::cell::RefCell;

use mcx::{Mcx, PgVec};
use types_core::{
    BlockNumber, ForkNumber, InvalidBlockNumber, ProcNumber, BLCKSZ, INVALID_PROC_NUMBER,
    MAX_FORKNUM,
};
use types_error::PgResult;
use types_storage::{RelFileLocator, RelFileLocatorBackend};

pub use storage_xlog::{
    smgr_redo, SMGR_TRUNCATE_ALL, XLOG_SMGR_CREATE, XLOG_SMGR_TRUNCATE,
};
const RM_SMGR_ID: u8 = 2;
const XLR_SPECIAL_REL_UPDATE: u8 = 0x01;

const RELPERSISTENCE_PERMANENT: u8 = b'p';
const RELPERSISTENCE_UNLOGGED: u8 = b'u';
const RELPERSISTENCE_TEMP: u8 = b't';

#[derive(Clone, Copy)]
struct PendingRelDelete {
    rlocator: RelFileLocator,
    proc_number: ProcNumber,
    at_commit: bool,
    nest_level: i32,
}

#[derive(Clone, Copy)]
struct PendingRelSync {
    rlocator: RelFileLocator,
    // A truncated relation must always take the fsync path (storage.c).
    is_truncated: bool,
}

thread_local! {
    static PENDING: RefCell<Vec<PendingRelDelete>> = const { RefCell::new(Vec::new()) };
    static PENDING_SYNCS: RefCell<Vec<PendingRelSync>> = const { RefCell::new(Vec::new()) };
    static WAL_SKIP_THRESHOLD: std::cell::Cell<i32> = const { std::cell::Cell::new(2048) };
}

pub fn AddPendingSync(rlocator: RelFileLocator) {
    PENDING_SYNCS.with_borrow_mut(|p| {
        debug_assert!(!p.iter().any(|s| s.rlocator == rlocator));
        p.push(PendingRelSync { rlocator, is_truncated: false });
    });
}

pub fn RelFileLocatorSkippingWAL(rlocator: RelFileLocator) -> bool {
    PENDING_SYNCS.with_borrow(|p| p.iter().any(|s| s.rlocator == rlocator))
}

pub fn SerializePendingSyncs() -> Vec<(RelFileLocator, bool)> {
    PENDING_SYNCS.with_borrow(|p| p.iter().map(|s| (s.rlocator, s.is_truncated)).collect())
}

pub fn RestorePendingSyncs(syncs: &[(RelFileLocator, bool)]) {
    PENDING_SYNCS.with_borrow_mut(|p| {
        debug_assert!(p.is_empty());
        p.extend(
            syncs
                .iter()
                .map(|&(rlocator, is_truncated)| PendingRelSync { rlocator, is_truncated }),
        );
    });
}

pub fn RelationCreateStorage(
    rlocator: RelFileLocator,
    relpersistence: u8,
    register_delete: bool,
) -> PgResult<RelFileLocatorBackend> {
    let (proc_number, needs_wal) = match relpersistence {
        RELPERSISTENCE_TEMP => (init_small::globals::MyProcNumber(), false),
        RELPERSISTENCE_UNLOGGED => (INVALID_PROC_NUMBER, false),
        RELPERSISTENCE_PERMANENT => (INVALID_PROC_NUMBER, true),
        _ => panic!("invalid relpersistence: {relpersistence}"),
    };

    let key = RelFileLocatorBackend { locator: rlocator, backend: proc_number };
    smgr::smgropen(rlocator, proc_number)?;
    smgr::smgrcreate(key, ForkNumber::MAIN_FORKNUM, false)?;

    if needs_wal {
        log_smgrcreate(&rlocator, ForkNumber::MAIN_FORKNUM)?;
    }

    if register_delete {
        PENDING.with_borrow_mut(|p| {
            p.insert(
                0,
                PendingRelDelete {
                    rlocator,
                    proc_number,
                    at_commit: false,
                    nest_level: xact::GetCurrentTransactionNestLevel(),
                },
            )
        });
    }

    if relpersistence == RELPERSISTENCE_PERMANENT && !transam_xlog::XLogIsNeeded() {
        AddPendingSync(rlocator);
    }

    Ok(key)
}

pub fn RelationTruncate(rel: &types_rel::RelationData<'_>, nblocks: BlockNumber) -> PgResult<()> {
    use std::sync::atomic::Ordering::Relaxed;
    use types_storage::{DELAY_CHKPT_COMPLETE, DELAY_CHKPT_START};

    let key = RelFileLocatorBackend { locator: rel.rd_locator.get(), backend: rel.rd_backend };
    smgr::smgropen(key.locator, key.backend)?;
    smgr::smgrsettargblock(key, InvalidBlockNumber);
    for fork_i in 0..=MAX_FORKNUM as i32 {
        let fork = ForkNumber::from_i32(fork_i).unwrap();
        smgr::smgr_set_cached_nblocks(key, fork, InvalidBlockNumber)?;
    }

    const NF: usize = MAX_FORKNUM as usize;
    let mut forks = [ForkNumber::MAIN_FORKNUM; NF];
    let mut old_blocks = [InvalidBlockNumber; NF];
    let mut blocks = [InvalidBlockNumber; NF];
    old_blocks[0] = smgr::smgrnblocks(key, ForkNumber::MAIN_FORKNUM)?;
    blocks[0] = nblocks;
    let mut nforks = 1;

    let mut need_fsm_vacuum = false;
    if smgr::smgrexists(key, ForkNumber::FSM_FORKNUM)? {
        let b = freespace::FreeSpaceMapPrepareTruncateRel(rel, nblocks)?;
        if b != InvalidBlockNumber {
            forks[nforks] = ForkNumber::FSM_FORKNUM;
            old_blocks[nforks] = smgr::smgrnblocks(key, ForkNumber::FSM_FORKNUM)?;
            blocks[nforks] = b;
            nforks += 1;
            need_fsm_vacuum = true;
        }
    }
    if smgr::smgrexists(key, ForkNumber::VISIBILITYMAP_FORKNUM)? {
        let b = visibilitymap::visibilitymap_prepare_truncate(rel, nblocks)?;
        if b != InvalidBlockNumber {
            forks[nforks] = ForkNumber::VISIBILITYMAP_FORKNUM;
            old_blocks[nforks] = smgr::smgrnblocks(key, ForkNumber::VISIBILITYMAP_FORKNUM)?;
            blocks[nforks] = b;
            nforks += 1;
        }
    }

    RelationPreTruncate(rel);

    // DELAY_CHKPT_COMPLETE: truncated-on-disk must precede the checkpoint
    // record; DELAY_CHKPT_START: the smgrtruncate sync request must be
    // processed before the checkpoint completes (storage.c).
    let proc = lmgr_proc::GetPGProcByNumber(lmgr_proc::MyProc().expect("MyProc is not set"));
    debug_assert_eq!(
        proc.delayChkptFlags.load(Relaxed) & (DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE),
        0
    );
    proc.delayChkptFlags.fetch_or(DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE, Relaxed);

    init_small::globals::StartCriticalSection();
    if relation_needs_wal(rel) {
        let lsn = log_smgrtruncate(&key.locator, nblocks)?;
        transam_xlog::XLogFlush(lsn)?;
    }
    smgr::smgrtruncate(key, &forks[..nforks], &old_blocks[..nforks], &blocks[..nforks])?;
    init_small::globals::EndCriticalSection();

    proc.delayChkptFlags.fetch_and(!(DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE), Relaxed);

    // Truncated-away pages were likely all-free and preferentially chosen:
    // rebuild the upper FSM levels now (FSM is un-WAL-logged, so no
    // checkpoint-delay concern).
    if need_fsm_vacuum {
        freespace::FreeSpaceMapVacuumRange(rel, nblocks, InvalidBlockNumber)?;
    }
    Ok(())
}

fn RelationPreTruncate(rel: &types_rel::RelationData<'_>) {
    let locator = rel.rd_locator.get();
    PENDING_SYNCS.with_borrow_mut(|p| {
        if let Some(s) = p.iter_mut().find(|s| s.rlocator == locator) {
            s.is_truncated = true;
        }
    });
}

// RelationNeedsWAL (rel.h).
fn relation_needs_wal(rel: &types_rel::RelationData<'_>) -> bool {
    rel.rd_rel.relpersistence == RELPERSISTENCE_PERMANENT
        && (transam_xlog::XLogIsNeeded()
            || (rel.rd_createSubid.get() == types_core::InvalidSubTransactionId
                && rel.rd_firstRelfilelocatorSubid.get() == types_core::InvalidSubTransactionId))
}

fn log_smgrtruncate(
    rlocator: &RelFileLocator,
    blkno: BlockNumber,
) -> PgResult<types_core::XLogRecPtr> {
    // xl_smgr_truncate image: blkno + RelFileLocator + flags.
    let mut xlrec = [0u8; 20];
    xlrec[0..4].copy_from_slice(&blkno.to_ne_bytes());
    xlrec[4..8].copy_from_slice(&rlocator.spcOid.to_ne_bytes());
    xlrec[8..12].copy_from_slice(&rlocator.dbOid.to_ne_bytes());
    xlrec[12..16].copy_from_slice(&rlocator.relNumber.to_ne_bytes());
    xlrec[16..20].copy_from_slice(&(SMGR_TRUNCATE_ALL as i32).to_ne_bytes());
    xloginsert_seams::xlog_insert_record::call(
        RM_SMGR_ID,
        XLOG_SMGR_TRUNCATE | XLR_SPECIAL_REL_UPDATE,
        0,
        &[&xlrec],
        &[],
    )
}

// RelationCopyStorage (storage.c): requires no dirty src data in shared
// buffers (callers flush first).
pub fn RelationCopyStorage(
    src: RelFileLocatorBackend,
    dst: RelFileLocatorBackend,
    fork_num: ForkNumber,
    relpersistence: u8,
) -> PgResult<()> {
    let copying_initfork =
        relpersistence == RELPERSISTENCE_UNLOGGED && fork_num == ForkNumber::INIT_FORKNUM;
    let use_wal = transam_xlog::XLogIsNeeded()
        && (relpersistence == RELPERSISTENCE_PERMANENT || copying_initfork);

    let mut bulkstate = bulkwrite::smgr_bulk_start_smgr(dst, fork_num, use_wal)?;
    let nblocks = smgr::smgrnblocks(src, fork_num)?;
    for blkno in 0..nblocks {
        if init_small::globals::InterruptPending() {
            panic!("CHECK_FOR_INTERRUPTS: ProcessInterrupts (tcop/postgres.c) unported");
        }
        let mut buf = bulkwrite::smgr_bulk_get_buf(&bulkstate);
        smgr::smgrread(src, fork_num, blkno, buf.page_mut())?;
        let mut piv_flags = bufmgr::PIV_LOG_WARNING;
        if bufmgr::ignore_checksum_failure() {
            piv_flags |= bufmgr::PIV_IGNORE_CHECKSUM_FAILURE;
        }
        let mut checksum_failure = false;
        let verified = bufmgr::page_is_verified(
            buf.page_mut().as_ptr(),
            blkno,
            piv_flags,
            Some(&mut checksum_failure),
        );
        if checksum_failure {
            pgstat::pgstat_prepare_report_checksum_failure(src.locator.dbOid);
            pgstat::pgstat_report_checksum_failures_in_db(src.locator.dbOid, 1);
        }
        if !verified {
            return Err(types_error::PgError::error(format!(
                "invalid page in block {blkno} of relation \"{}\"",
                bufmgr::relpath_backend_desc(src.locator, src.backend, fork_num)
            ))
            .with_sqlstate(types_error::ERRCODE_DATA_CORRUPTED)
            .into());
        }
        bulkwrite::smgr_bulk_write(&mut bulkstate, blkno, buf, false)?;
    }
    bulkwrite::smgr_bulk_finish(bulkstate)
}

pub fn RelationDropStorage(rel: &types_rel::RelationData<'_>) -> PgResult<()> {
    PENDING.with_borrow_mut(|p| {
        p.insert(
            0,
            PendingRelDelete {
                rlocator: rel.rd_locator.get(),
                proc_number: rel.rd_backend,
                at_commit: true,
                nest_level: xact::GetCurrentTransactionNestLevel(),
            },
        )
    });
    smgr::RelationCloseSmgr(rel)
}

pub fn log_smgrcreate(rlocator: &RelFileLocator, fork_num: ForkNumber) -> PgResult<()> {
    // xl_smgr_create image: RelFileLocator{spcOid,dbOid,relNumber} + ForkNumber.
    let mut xlrec = [0u8; 16];
    xlrec[0..4].copy_from_slice(&rlocator.spcOid.to_ne_bytes());
    xlrec[4..8].copy_from_slice(&rlocator.dbOid.to_ne_bytes());
    xlrec[8..12].copy_from_slice(&rlocator.relNumber.to_ne_bytes());
    xlrec[12..16].copy_from_slice(&(fork_num as i32).to_ne_bytes());
    xloginsert_seams::xlog_insert_record::call(
        RM_SMGR_ID,
        XLOG_SMGR_CREATE | XLR_SPECIAL_REL_UPDATE,
        0,
        &[&xlrec],
        &[],
    )?;
    Ok(())
}

pub fn smgrDoPendingDeletes(is_commit: bool) -> PgResult<()> {
    let nest_level = xact::GetCurrentTransactionNestLevel();
    let mut to_unlink: Vec<RelFileLocatorBackend> = Vec::new();
    PENDING.with_borrow_mut(|p| {
        p.retain(|pending| {
            if pending.nest_level < nest_level {
                return true;
            }
            if pending.at_commit == is_commit {
                to_unlink.push(RelFileLocatorBackend {
                    locator: pending.rlocator,
                    backend: pending.proc_number,
                });
            }
            false
        });
    });
    if !to_unlink.is_empty() {
        for key in &to_unlink {
            smgr::smgropen(key.locator, key.backend)?;
        }
        smgr::smgrdounlinkall(&to_unlink, false)?;
    }
    Ok(())
}

pub fn smgrGetPendingDeletes<'mcx>(
    mcx: Mcx<'mcx>,
    for_commit: bool,
) -> PgResult<PgVec<'mcx, RelFileLocator>> {
    // C filters on nestLevel >= GetCurrentTransactionNestLevel(): a subxact's
    // abort record must carry only that subxact's creations, never the outer
    // transaction's (redo would drop live relfilenodes).
    let nest_level = xact::GetCurrentTransactionNestLevel();
    let mut out = PgVec::new_in(mcx);
    PENDING.with_borrow(|p| {
        for pending in p {
            if pending.nest_level >= nest_level
                && pending.at_commit == for_commit
                && pending.proc_number == INVALID_PROC_NUMBER
            {
                out.push(pending.rlocator);
            }
        }
    });
    Ok(out)
}

pub fn smgrDoPendingSyncs(is_commit: bool, is_parallel_worker: bool) -> PgResult<()> {
    let mut syncs = PENDING_SYNCS.with_borrow_mut(std::mem::take);
    if syncs.is_empty() || !is_commit || is_parallel_worker {
        return Ok(());
    }

    PENDING.with_borrow(|p| {
        syncs.retain(|s| !p.iter().any(|d| d.at_commit && d.rlocator == s.rlocator));
    });

    let mut srels: Vec<RelFileLocatorBackend> = Vec::new();
    for sync in &syncs {
        let key = RelFileLocatorBackend { locator: sync.rlocator, backend: INVALID_PROC_NUMBER };
        smgr::smgropen(sync.rlocator, INVALID_PROC_NUMBER)?;

        let mut nblocks = [InvalidBlockNumber; MAX_FORKNUM as usize + 1];
        let mut total_blocks: u64 = 0;
        if !sync.is_truncated {
            for fork_i in 0..=MAX_FORKNUM as i32 {
                let fork = ForkNumber::from_i32(fork_i).unwrap();
                if smgr::smgrexists(key, fork)? {
                    debug_assert!(fork != ForkNumber::INIT_FORKNUM);
                    let n = smgr::smgrnblocks(key, fork)?;
                    nblocks[fork_i as usize] = n;
                    total_blocks += n as u64;
                }
            }
        }

        let threshold =
            WAL_SKIP_THRESHOLD.with(|c| c.get()) as u64 * 1024 / BLCKSZ as u64;
        if sync.is_truncated || total_blocks >= threshold {
            srels.push(key);
        } else {
            for fork_i in 0..=MAX_FORKNUM as i32 {
                let n = nblocks[fork_i as usize];
                if n == InvalidBlockNumber {
                    continue;
                }
                let rel = xlogutils::CreateFakeRelcacheEntry(sync.rlocator);
                xloginsert::log_newpage_range(
                    &rel,
                    ForkNumber::from_i32(fork_i).unwrap(),
                    0,
                    n as BlockNumber,
                    false,
                )?;
            }
        }
    }

    if !srels.is_empty() {
        smgr::smgrdosyncall(&srels)?;
    }
    Ok(())
}

pub fn AtSubCommit_smgr() {
    let nest_level = xact::GetCurrentTransactionNestLevel();
    PENDING.with_borrow_mut(|p| {
        for pending in p.iter_mut() {
            if pending.nest_level >= nest_level {
                pending.nest_level = nest_level - 1;
            }
        }
    });
}

pub fn AtSubAbort_smgr() -> PgResult<()> {
    smgrDoPendingDeletes(false)
}

// The physical deletes belong to the 2PC state file: StartPrepare serialized
// them via smgrGetPendingDeletes, FinishPreparedTransaction replays them via
// DropRelationFiles. C just frees the local list here.
pub fn PostPrepare_smgr() {
    PENDING.with_borrow_mut(|p| p.clear());
}

pub fn RelationPreserveStorage(rlocator: RelFileLocator, at_commit: bool) {
    PENDING.with_borrow_mut(|p| {
        p.retain(|pending| !(pending.rlocator == rlocator && pending.at_commit == at_commit))
    });
}

pub fn DropRelationFiles(delrels: &[RelFileLocator], is_redo: bool) -> PgResult<()> {
    let mut srels = Vec::with_capacity(delrels.len());
    for locator in delrels {
        let key = RelFileLocatorBackend { locator: *locator, backend: INVALID_PROC_NUMBER };
        smgr::smgropen(key.locator, key.backend)?;
        srels.push(key);
    }
    smgr::smgrdounlinkall(&srels, is_redo)?;
    Ok(())
}

pub fn init_seams() {
    guc_tables::vars::wal_skip_threshold.install(guc_tables::GucVarAccessors {
        get: || WAL_SKIP_THRESHOLD.with(|c| c.get()),
        set: |v| WAL_SKIP_THRESHOLD.with(|c| c.set(v)),
    });
    catalog_storage_seams::smgr_get_pending_deletes::set(smgrGetPendingDeletes);
    catalog_storage_seams::smgr_do_pending_deletes::set(smgrDoPendingDeletes);
    catalog_storage_seams::smgr_do_pending_syncs::set(smgrDoPendingSyncs);
    catalog_storage_seams::rel_file_locator_skipping_wal::set(RelFileLocatorSkippingWAL);
    catalog_storage_seams::at_subcommit_smgr::set(AtSubCommit_smgr);
    catalog_storage_seams::at_subabort_smgr::set(AtSubAbort_smgr);
    catalog_storage_seams::post_prepare_smgr::set(PostPrepare_smgr);
    catalog_storage_seams::drop_relation_files::set(DropRelationFiles);
    catalog_storage_seams::relation_preserve_storage::set(RelationPreserveStorage);
    catalog_storage_seams::relation_create_storage::set(
        |rlocator, relpersistence, register_delete| {
            RelationCreateStorage(rlocator, relpersistence, register_delete).map(|_| ())
        },
    );
    catalog_storage_seams::log_smgrcreate::set(|rlocator, fork_num| {
        log_smgrcreate(&rlocator, fork_num)
    });
}

#[cfg(test)]
mod tests;
