// The public surface keeps the PostgreSQL function names
// (`RelationCreateStorage`, `smgrDoPendingDeletes`, …) so callers map 1:1 onto
// storage.c.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! Port of `backend/catalog/storage.c` — code to create and destroy physical
//! storage for relations.
//!
//! The two C file-scope statics — the `pendingDeletes` linked list and the
//! `pendingSyncHash` hash — are backend-local transaction state (not shared
//! memory; in C they live in TopMemoryContext / TopTransactionContext). They
//! are kept in-crate as `thread_local!` state: `pendingDeletes` becomes a
//! `Vec<PendingRelDelete>` (index 0 == the C list head, since C prepends), and
//! `pendingSyncHash` becomes an `Option<HashMap<..>>` (`None` == the C `NULL`).

use std::cell::RefCell;
use std::collections::HashMap;

use types_core::primitive::{
    BlockNumber, ForkNumber, Oid, ProcNumber, Size, BLCKSZ, INVALID_PROC_NUMBER,
};
use types_core::primitive::ForkNumber::{
    FSM_FORKNUM, INIT_FORKNUM, MAIN_FORKNUM, VISIBILITYMAP_FORKNUM,
};
use types_core::primitive::InvalidBlockNumber;
use types_error::{PgError, PgResult};
use types_error::error::{
    ERRCODE_DATA_CORRUPTED, ERRCODE_OUT_OF_MEMORY, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR, PANIC,
};
use types_storage::storage::{RelFileLocator, RelFileLocatorEquals};
use types_storage::relfilelocator::RelFileLocatorBackend;

use mcx::Mcx;

use backend_catalog_storage_seams as storage_seam;

// Direct deps (real fns).
use backend_storage_smgr_smgr as smgr;
use backend_access_transam_xloginsert as xloginsert;
use backend_access_transam_xlogutils as xlogutils;
use backend_storage_freespace as freespace;
use backend_access_heap_visibilitymap as visibilitymap;
use backend_storage_page as page;
use backend_common_relpath as relpath;
use backend_access_table_table as table;

// Outward seams.
use backend_storage_smgr_seams as smgr_seam;
use backend_access_transam_xlog_seams as xlog_seam;
use backend_access_transam_xact_seams as xact_seam;
use backend_storage_lmgr_proc_seams as proc_seam;
use backend_utils_cache_relcache_seams as relcache_seam;
use backend_utils_init_miscinit_seams as miscinit_seam;
use backend_utils_cache_syscache_seams as syscache_seam;
use backend_catalog_indexing_seams as indexing_seam;
use backend_storage_smgr_bulkwrite_seams as bulkwrite_seam;
use backend_utils_activity_pgstat_seams as pgstat_seam;

/* ---------------------------------------------------------------------------
 * Constants from headers.
 * ------------------------------------------------------------------------- */

/// `RELPERSISTENCE_PERMANENT` (pg_class.h: 'p').
const RELPERSISTENCE_PERMANENT: i8 = b'p' as i8;
/// `RELPERSISTENCE_UNLOGGED` (pg_class.h: 'u').
const RELPERSISTENCE_UNLOGGED: i8 = b'u' as i8;
/// `RELPERSISTENCE_TEMP` (pg_class.h: 't').
const RELPERSISTENCE_TEMP: i8 = b't' as i8;

/// `RELKIND_SEQUENCE` (pg_class.h: 'S').
const RELKIND_SEQUENCE: i8 = b'S' as i8;

/// `XLOG_SMGR_CREATE` (catalog/storage_xlog.h).
const XLOG_SMGR_CREATE: u8 = 0x10;
/// `XLOG_SMGR_TRUNCATE` (catalog/storage_xlog.h).
const XLOG_SMGR_TRUNCATE: u8 = 0x20;

/// `SMGR_TRUNCATE_HEAP` (storage_xlog.h).
const SMGR_TRUNCATE_HEAP: i32 = 0x0001;
/// `SMGR_TRUNCATE_VM` (storage_xlog.h).
const SMGR_TRUNCATE_VM: i32 = 0x0002;
/// `SMGR_TRUNCATE_FSM` (storage_xlog.h).
const SMGR_TRUNCATE_FSM: i32 = 0x0004;
/// `SMGR_TRUNCATE_ALL` (storage_xlog.h).
const SMGR_TRUNCATE_ALL: i32 = SMGR_TRUNCATE_HEAP | SMGR_TRUNCATE_VM | SMGR_TRUNCATE_FSM;

/// `RM_SMGR_ID` — the Storage resource manager.
const RM_SMGR_ID: types_core::primitive::RmgrId = types_wal::wal::RM_SMGR_ID;
/// `XLR_SPECIAL_REL_UPDATE`.
const XLR_SPECIAL_REL_UPDATE: u8 = types_wal::wal::XLR_SPECIAL_REL_UPDATE;
/// `XLR_INFO_MASK`.
const XLR_INFO_MASK: u8 = types_wal::wal::XLR_INFO_MASK;

/// `RelationRelationId` (pg_class OID).
const RelationRelationId: Oid = 1259;
/// `RowExclusiveLock` (lockdefs.h).
const RowExclusiveLock: types_storage::lock::LOCKMODE = types_storage::lock::RowExclusiveLock;
/// `RELOID` syscache id.
const RELOID: i32 = types_syscache::syscache_ids::RELOID;

/// `PIV_LOG_WARNING` (bufpage.h).
const PIV_LOG_WARNING: i32 = types_storage::bufpage::PIV_LOG_WARNING;
/// `PIV_IGNORE_CHECKSUM_FAILURE` (bufpage.h).
const PIV_IGNORE_CHECKSUM_FAILURE: i32 = types_storage::bufpage::PIV_IGNORE_CHECKSUM_FAILURE;

/// `BlockNumberIsValid(b)` (block.h): `b != InvalidBlockNumber`.
#[inline]
fn BlockNumberIsValid(b: BlockNumber) -> bool {
    b != InvalidBlockNumber
}

/// OOM error matching `mcx.oom`-style failure.
fn oom() -> PgError {
    PgError::new(ERROR, "out of memory").with_sqlstate(ERRCODE_OUT_OF_MEMORY)
}

/// Convert the WAL-record `RelFileLocator` (`types_wal`) to the storage-layer
/// one (`types_storage`); the two are byte-identical (three `Oid`s) but live in
/// different type crates.
#[inline]
fn from_wal_locator(loc: types_wal::RelFileLocator) -> RelFileLocator {
    RelFileLocator {
        spcOid: loc.spc_oid(),
        dbOid: loc.db_oid(),
        relNumber: loc.rel_number(),
    }
}

/* ---------------------------------------------------------------------------
 * GUC variable.
 * ------------------------------------------------------------------------- */

// `int wal_skip_threshold = 2048;` (in kilobytes). Backend-local GUC.
thread_local! {
    static WAL_SKIP_THRESHOLD: RefCell<i32> = const { RefCell::new(2048) };
}

/// Read `wal_skip_threshold`.
pub fn wal_skip_threshold() -> i32 {
    WAL_SKIP_THRESHOLD.with(|v| *v.borrow())
}

/// Set `wal_skip_threshold` (the GUC assign hook).
pub fn set_wal_skip_threshold(kilobytes: i32) {
    WAL_SKIP_THRESHOLD.with(|v| *v.borrow_mut() = kilobytes);
}

/* ---------------------------------------------------------------------------
 * Backend-local transaction state (the C file-scope statics).
 *
 *   static PendingRelDelete *pendingDeletes = NULL;
 *   static HTAB *pendingSyncHash = NULL;
 * ------------------------------------------------------------------------- */

/// `struct PendingRelDelete` (storage.c:62). `next` is implicit in the `Vec`
/// ordering (index 0 == list head).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingRelDelete {
    /// relation that may need to be deleted.
    rlocator: RelFileLocator,
    /// `INVALID_PROC_NUMBER` if not a temp rel.
    proc_number: ProcNumber,
    /// `true` = delete at commit; `false` = delete at abort.
    at_commit: bool,
    /// xact nesting level of request.
    nest_level: i32,
}

/// `struct PendingRelSync` (storage.c:71). The `rlocator` is the hash key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingRelSync {
    /// Has the file experienced truncation?
    is_truncated: bool,
}

/// Owner of the two backend-local statics.
struct PendingState {
    /// `pendingDeletes` linked list; index 0 is the list head.
    deletes: Vec<PendingRelDelete>,
    /// `pendingSyncHash`; `None` == the C `NULL` (not yet created / cleared).
    sync_hash: Option<HashMap<RelFileLocator, PendingRelSync>>,
}

thread_local! {
    static PENDING: RefCell<PendingState> = const {
        RefCell::new(PendingState { deletes: Vec::new(), sync_hash: None })
    };
}

/* ---------------------------------------------------------------------------
 * AddPendingSync (storage.c:86) — static.
 * ------------------------------------------------------------------------- */

/// `AddPendingSync` — queue an at-commit fsync.
fn add_pending_sync(rlocator: RelFileLocator) -> PgResult<()> {
    PENDING.with(|p| {
        let mut st = p.borrow_mut();
        // create the hash if not yet
        let hash = st.sync_hash.get_or_insert_with(HashMap::new);
        // pending = hash_search(..., HASH_ENTER, &found); Assert(!found);
        debug_assert!(!hash.contains_key(&rlocator));
        hash.try_reserve(1).map_err(|_| oom())?;
        // pending->is_truncated = false;
        hash.insert(rlocator, PendingRelSync { is_truncated: false });
        Ok(())
    })
}

/* ---------------------------------------------------------------------------
 * RelationCreateStorage (storage.c:122)
 * ------------------------------------------------------------------------- */

/// `RelationCreateStorage` — create physical storage for a relation.
///
/// Returns the freshly opened smgr identity (`RelFileLocatorBackend`). C returns
/// an `SMgrRelation`; in this repo smgr operations key off the
/// `(RelFileLocator, ProcNumber)` pair directly, so we return that key.
pub fn RelationCreateStorage(
    rlocator: RelFileLocator,
    relpersistence: i8,
    register_delete: bool,
) -> PgResult<RelFileLocatorBackend> {
    // Assert(!IsInParallelMode());  /* couldn't update pendingSyncHash */
    debug_assert!(!xact_seam::is_in_parallel_mode::call());

    let proc_number: ProcNumber;
    let needs_wal: bool;

    match relpersistence {
        RELPERSISTENCE_TEMP => {
            proc_number = proc_number_for_temp_relations();
            needs_wal = false;
        }
        RELPERSISTENCE_UNLOGGED => {
            proc_number = INVALID_PROC_NUMBER;
            needs_wal = false;
        }
        RELPERSISTENCE_PERMANENT => {
            proc_number = INVALID_PROC_NUMBER;
            needs_wal = true;
        }
        _ => {
            // elog(ERROR, "invalid relpersistence: %c", relpersistence);
            return Err(PgError::new(
                ERROR,
                format!("invalid relpersistence: {}", relpersistence as u8 as char),
            ));
        }
    }

    // srel = smgropen(rlocator, procNumber);
    let srel = smgr::smgropen(rlocator, proc_number)?;
    let key = srel.smgr_rlocator;
    // smgrcreate(srel, MAIN_FORKNUM, false);
    smgr::smgrcreate(key, MAIN_FORKNUM, false)?;

    if needs_wal {
        // log_smgrcreate(&srel->smgr_rlocator.locator, MAIN_FORKNUM);
        log_smgrcreate(key.locator, MAIN_FORKNUM)?;
    }

    // Add the relation to the list of stuff to delete at abort, if asked.
    if register_delete {
        let nest_level = xact_seam::get_current_transaction_nest_level::call();
        PENDING.with(|p| -> PgResult<()> {
            let mut st = p.borrow_mut();
            // C prepends: pending->next = pendingDeletes; pendingDeletes = pending.
            st.deletes.try_reserve(1).map_err(|_| oom())?;
            st.deletes.insert(
                0,
                PendingRelDelete {
                    rlocator,
                    proc_number,
                    at_commit: false, // delete if abort
                    nest_level,
                },
            );
            Ok(())
        })?;
    }

    if relpersistence == RELPERSISTENCE_PERMANENT && !xlog_is_needed()? {
        debug_assert_eq!(proc_number, INVALID_PROC_NUMBER);
        add_pending_sync(rlocator)?;
    }

    Ok(key)
}

/// `XLogIsNeeded()` (xlog.h) — `wal_level >= WAL_LEVEL_REPLICA`.
fn xlog_is_needed() -> PgResult<bool> {
    Ok(xlog_seam::wal_level::call() >= types_wal::xlog_consts::WAL_LEVEL_REPLICA)
}

/// `ProcNumberForTempRelations()` (storage/procnumber.h): our own proc number
/// normally, but parallel workers use their leader's.
pub fn proc_number_for_temp_relations() -> ProcNumber {
    let leader = backend_access_transam_parallel_rt_seams::parallel_leader_proc_number::call();
    if leader == INVALID_PROC_NUMBER {
        backend_utils_init_small_seams::my_proc_number::call()
    } else {
        leader
    }
}

/* ---------------------------------------------------------------------------
 * log_smgrcreate (storage.c:187)
 * ------------------------------------------------------------------------- */

/// `log_smgrcreate` — `XLogInsert` of an `XLOG_SMGR_CREATE` record to WAL.
pub fn log_smgrcreate(rlocator: RelFileLocator, fork_num: ForkNumber) -> PgResult<()> {
    // xl_smgr_create xlrec; xlrec.rlocator = *rlocator; xlrec.forkNum = forkNum;
    // Serialize: rlocator (spcOid, dbOid, relNumber) @0, forkNum (int) @12.
    let mut xlrec = [0u8; 16];
    xlrec[0..4].copy_from_slice(&rlocator.spcOid.to_ne_bytes());
    xlrec[4..8].copy_from_slice(&rlocator.dbOid.to_ne_bytes());
    xlrec[8..12].copy_from_slice(&rlocator.relNumber.to_ne_bytes());
    xlrec[12..16].copy_from_slice(&(fork_num as i32).to_ne_bytes());

    // XLogBeginInsert(); XLogRegisterData(&xlrec, sizeof(xlrec));
    xloginsert::XLogBeginInsert()?;
    xloginsert::XLogRegisterData(&xlrec)?;
    // XLogInsert(RM_SMGR_ID, XLOG_SMGR_CREATE | XLR_SPECIAL_REL_UPDATE);
    xloginsert::XLogInsert(RM_SMGR_ID, XLOG_SMGR_CREATE | XLR_SPECIAL_REL_UPDATE)?;
    Ok(())
}

/* ---------------------------------------------------------------------------
 * RelationDropStorage (storage.c:207)
 * ------------------------------------------------------------------------- */

/// `RelationDropStorage` — schedule unlinking of physical storage at commit.
///
/// The relcache owns the relation entry, so its physical identity
/// (`rd_locator`, `rd_backend`) is passed explicitly.
pub fn relation_drop_storage(rlocator: RelFileLocator, backend: ProcNumber) -> PgResult<()> {
    let nest_level = xact_seam::get_current_transaction_nest_level::call();

    // Add the relation to the list of stuff to delete at commit.
    PENDING.with(|p| -> PgResult<()> {
        let mut st = p.borrow_mut();
        st.deletes.try_reserve(1).map_err(|_| oom())?;
        st.deletes.insert(
            0,
            PendingRelDelete {
                rlocator,
                proc_number: backend,
                at_commit: true, // delete if commit
                nest_level,
            },
        );
        Ok(())
    })?;

    // NOTE: if the relation was created in this transaction, it is now present
    // twice (atCommit true and false); smgrDoPendingDeletes handles both.

    // RelationCloseSmgr(rel);
    smgr_seam::relation_close_smgr::call(RelFileLocatorBackend {
        locator: rlocator,
        backend,
    });
    Ok(())
}

/// The binary-upgrade old-storage drop in `RelationSetNewRelfilenumber`:
/// `srel = smgropen(rlocator, backend); smgrdounlinkall(&srel, 1, false);
/// smgrclose(srel)`.
pub fn smgr_unlink_relation_now(rlocator: RelFileLocator, backend: ProcNumber) -> PgResult<()> {
    let srel = smgr::smgropen(rlocator, backend)?;
    let key = srel.smgr_rlocator;
    smgr::smgrdounlinkall(&[key], false)?;
    smgr::smgrclose(key)
}

/* ---------------------------------------------------------------------------
 * RelationPreserveStorage (storage.c:252)
 * ------------------------------------------------------------------------- */

/// `RelationPreserveStorage` — mark a relation as not to be deleted after all.
/// No-op if the relation is not among those scheduled for deletion.
pub fn RelationPreserveStorage(rlocator: RelFileLocator, at_commit: bool) -> PgResult<()> {
    PENDING.with(|p| {
        let mut st = p.borrow_mut();
        // for (pending = pendingDeletes; ...) { if (match) unlink+pfree; else keep; }
        st.deletes.retain(|pending| {
            !(RelFileLocatorEquals(&rlocator, &pending.rlocator) && pending.at_commit == at_commit)
        });
    });
    Ok(())
}

/* ---------------------------------------------------------------------------
 * RelationTruncate (storage.c:289)
 * ------------------------------------------------------------------------- */

/// `RelationTruncate` — physically truncate a relation to `nblocks` blocks.
pub fn RelationTruncate(rel: &types_rel::Relation<'_>, nblocks: BlockNumber) -> PgResult<()> {
    let mut need_fsm_vacuum = false;

    let key = RelFileLocatorBackend {
        locator: rel.rd_locator,
        backend: rel.rd_backend,
    };

    // reln = RelationGetSmgr(rel);  reln->smgr_targblock = InvalidBlockNumber;
    // for (i = 0; i <= MAX_FORKNUM; ++i) reln->smgr_cached_nblocks[i] = Invalid.
    // In this repo the smgr cache is keyed; resetting the targblock/cache is a
    // no-op against the value-keyed model (smgropen returns a fresh snapshot),
    // but call smgrnblocks below off the live key.

    let mut forks: Vec<ForkNumber> = Vec::new();
    let mut old_blocks: Vec<BlockNumber> = Vec::new();
    let mut blocks: Vec<BlockNumber> = Vec::new();

    // Prepare for truncation of MAIN fork of the relation.
    forks.push(MAIN_FORKNUM);
    old_blocks.push(smgr::smgrnblocks(key, MAIN_FORKNUM)?);
    blocks.push(nblocks);

    // Prepare for truncation of the FSM if it exists.
    let fsm = smgr::smgrexists(key, FSM_FORKNUM)?;
    if fsm {
        let b = freespace::FreeSpaceMapPrepareTruncateRel(rel, nblocks)?;
        if BlockNumberIsValid(b) {
            forks.push(FSM_FORKNUM);
            old_blocks.push(smgr::smgrnblocks(key, FSM_FORKNUM)?);
            blocks.push(b);
            need_fsm_vacuum = true;
        }
    }

    // Prepare for truncation of the visibility map too if it exists.
    let vm = smgr::smgrexists(key, VISIBILITYMAP_FORKNUM)?;
    if vm {
        let b = visibilitymap::visibilitymap_prepare_truncate(rel, nblocks)?;
        if BlockNumberIsValid(b) {
            forks.push(VISIBILITYMAP_FORKNUM);
            old_blocks.push(smgr::smgrnblocks(key, VISIBILITYMAP_FORKNUM)?);
            blocks.push(b);
        }
    }

    RelationPreTruncate(rel)?;

    // Assert((MyProc->delayChkptFlags & (...)) == 0);
    // MyProc->delayChkptFlags |= DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE;
    proc_seam::set_delay_chkpt_start::call(true);
    proc_seam::set_delay_chkpt_complete::call(true);

    // WAL-log the truncation first and then truncate in a critical section.
    miscinit_seam::start_crit_section::call();

    if relcache_seam::relation_needs_wal::call(rel) {
        // xlrec.blkno = nblocks; xlrec.rlocator = rel->rd_locator;
        // xlrec.flags = SMGR_TRUNCATE_ALL;
        let lsn = log_smgr_truncate(rel.rd_locator, nblocks, SMGR_TRUNCATE_ALL)?;
        // XLogFlush(lsn);
        xlog_seam::xlog_flush::call(lsn)?;
    }

    // Remove buffers then truncate the files on disk.
    smgr::smgrtruncate(key, &forks, &old_blocks, &blocks)?;

    miscinit_seam::end_crit_section::call();

    // We've done all the critical work, so checkpoints are OK now.
    proc_seam::set_delay_chkpt_start::call(false);
    proc_seam::set_delay_chkpt_complete::call(false);

    // Update upper-level FSM pages to account for the truncation.
    if need_fsm_vacuum {
        freespace::FreeSpaceMapVacuumRange(rel, nblocks, InvalidBlockNumber)?;
    }

    Ok(())
}

/// Emit an `XLOG_SMGR_TRUNCATE` record (the inline xlog leg of `RelationTruncate`
/// / a part of `smgr_redo`'s WAL emission). Serializes `xl_smgr_truncate`:
/// blkno@0, rlocator@4 (three Oids), flags@16.
fn log_smgr_truncate(
    rlocator: RelFileLocator,
    blkno: BlockNumber,
    flags: i32,
) -> PgResult<types_core::primitive::XLogRecPtr> {
    let mut xlrec = [0u8; 20];
    xlrec[0..4].copy_from_slice(&blkno.to_ne_bytes());
    xlrec[4..8].copy_from_slice(&rlocator.spcOid.to_ne_bytes());
    xlrec[8..12].copy_from_slice(&rlocator.dbOid.to_ne_bytes());
    xlrec[12..16].copy_from_slice(&rlocator.relNumber.to_ne_bytes());
    xlrec[16..20].copy_from_slice(&flags.to_ne_bytes());

    xloginsert::XLogBeginInsert()?;
    xloginsert::XLogRegisterData(&xlrec)?;
    xloginsert::XLogInsert(RM_SMGR_ID, XLOG_SMGR_TRUNCATE | XLR_SPECIAL_REL_UPDATE)
}

/* ---------------------------------------------------------------------------
 * RelationPreTruncate (storage.c:450)
 * ------------------------------------------------------------------------- */

/// `RelationPreTruncate` — AM-independent work before a physical truncation.
pub fn RelationPreTruncate(rel: &types_rel::Relation<'_>) -> PgResult<()> {
    // if (!pendingSyncHash) return;
    let has_hash = PENDING.with(|p| p.borrow().sync_hash.is_some());
    if !has_hash {
        return Ok(());
    }

    // pending = hash_search(pendingSyncHash,
    //   &(RelationGetSmgr(rel)->smgr_rlocator.locator), HASH_FIND, NULL);
    let key = rel.rd_locator;

    // if (pending) pending->is_truncated = true;
    PENDING.with(|p| {
        let mut st = p.borrow_mut();
        if let Some(hash) = st.sync_hash.as_mut() {
            if let Some(pending) = hash.get_mut(&key) {
                pending.is_truncated = true;
            }
        }
    });
    Ok(())
}

/* ---------------------------------------------------------------------------
 * RelationCopyStorage (storage.c:478)
 * ------------------------------------------------------------------------- */

/// `RelationCopyStorage` — copy a fork's data, block by block.
pub fn RelationCopyStorage<'mcx>(
    mcx: Mcx<'mcx>,
    src: RelFileLocatorBackend,
    dst: RelFileLocatorBackend,
    fork_num: ForkNumber,
    relpersistence: i8,
) -> PgResult<()> {
    // The init fork for an unlogged relation must be treated like a normal rel.
    let copying_initfork = relpersistence == RELPERSISTENCE_UNLOGGED && fork_num == INIT_FORKNUM;

    // use_wal = XLogIsNeeded() && (PERMANENT || copying_initfork)
    let use_wal = xlog_is_needed()?
        && (relpersistence == RELPERSISTENCE_PERMANENT || copying_initfork);

    // bulkstate = smgr_bulk_start_smgr(dst, forkNum, use_wal);
    let mut bulkstate = bulkwrite_seam::smgr_bulk_start_smgr::call(mcx, dst, fork_num, use_wal)?;

    // nblocks = smgrnblocks(src, forkNum);
    let nblocks = smgr::smgrnblocks(src, fork_num)?;

    let mut blkno: BlockNumber = 0;
    while blkno < nblocks {
        // CHECK_FOR_INTERRUPTS();
        miscinit_seam::check_for_interrupts::call()?;

        // buf = smgr_bulk_get_buf(bulkstate); smgrread(src, forkNum, blkno, buf);
        let mut buf = bulkwrite_seam::smgr_bulk_get_buf::call(mcx, &mut bulkstate)?;
        smgr_seam::smgr_read::call(src.locator, src.backend, fork_num, blkno, &mut buf[..])?;

        let mut piv_flags = PIV_LOG_WARNING;
        if ignore_checksum_failure()? {
            piv_flags |= PIV_IGNORE_CHECKSUM_FAILURE;
        }
        // verified = PageIsVerified(buf, blkno, piv_flags, &checksum_failure);
        let (verified, checksum_failure) = {
            let p = page::PageRef::new(&buf[..])?;
            page::PageIsVerified(&p, blkno, piv_flags)?
        };

        if checksum_failure {
            // pgstat_prepare_report_checksum_failure(rloc.locator.dbOid);
            // pgstat_report_checksum_failures_in_db(rloc.locator.dbOid, 1);
            pgstat_seam::pgstat_prepare_report_checksum_failure::call(src.locator.dbOid)?;
            pgstat_seam::pgstat_report_checksum_failures_in_db::call(src.locator.dbOid, 1)?;
        }

        if !verified {
            // Capture the file path before invoking ereport.
            let relpath = relpath::relpathbackend(src.locator, src.backend, fork_num);
            return Err(PgError::new(
                ERROR,
                format!("invalid page in block {blkno} of relation \"{relpath}\""),
            )
            .with_sqlstate(ERRCODE_DATA_CORRUPTED));
        }

        // smgr_bulk_write(bulkstate, blkno, buf, false);
        bulkwrite_seam::smgr_bulk_write::call(&mut bulkstate, blkno, buf, false)?;

        blkno += 1;
    }
    // smgr_bulk_finish(bulkstate);
    bulkwrite_seam::smgr_bulk_finish::call(bulkstate)
}

/// `ignore_checksum_failure` GUC (bufpage.c) — backend-local bool.
fn ignore_checksum_failure() -> PgResult<bool> {
    Ok(backend_utils_misc_guc_tables::vars::ignore_checksum_failure.read())
}

/* ---------------------------------------------------------------------------
 * RelFileLocatorSkippingWAL (storage.c:573)
 * ------------------------------------------------------------------------- */

/// `RelFileLocatorSkippingWAL` — check if a `BM_PERMANENT` relfilelocator skips
/// WAL.
pub fn rel_file_locator_skipping_wal(rlocator: RelFileLocator) -> bool {
    // if (!pendingSyncHash || hash_search(..., HASH_FIND, NULL) == NULL)
    //     return false;  return true;
    PENDING.with(|p| {
        let st = p.borrow();
        match &st.sync_hash {
            None => false,
            Some(hash) => hash.contains_key(&rlocator),
        }
    })
}

/* ---------------------------------------------------------------------------
 * EstimatePendingSyncsSpace (storage.c:587)
 * ------------------------------------------------------------------------- */

/// Size of one serialized `RelFileLocator`: three `Oid`s (`sizeof(RelFileLocator)`).
const RelFileLocatorWireSize: Size = 3 * core::mem::size_of::<Oid>();

/// `EstimatePendingSyncsSpace` — estimate space to pass syncs to parallel
/// workers.
pub fn EstimatePendingSyncsSpace() -> PgResult<Size> {
    // entries = pendingSyncHash ? hash_get_num_entries(pendingSyncHash) : 0;
    let entries: i64 = PENDING.with(|p| {
        let st = p.borrow();
        match &st.sync_hash {
            None => 0,
            Some(hash) => hash.len() as i64,
        }
    });
    // return mul_size(1 + entries, sizeof(RelFileLocator));
    mul_size((1 + entries) as Size, RelFileLocatorWireSize)
}

/// `mul_size(s1, s2)` (shmem.c) — overflow-checked size multiply.
fn mul_size(s1: Size, s2: Size) -> PgResult<Size> {
    s1.checked_mul(s2).ok_or_else(|| {
        PgError::new(ERROR, "requested shared memory size overflows size_t")
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
    })
}

/* ---------------------------------------------------------------------------
 * SerializePendingSyncs (storage.c:600)
 * ------------------------------------------------------------------------- */

/// `SerializePendingSyncs` — serialize syncs for parallel workers into `dest`.
///
/// The idiomatic surface fills the caller-provided slice (laid out as
/// `RelFileLocator` records). The active set is the pending syncs minus the
/// relations the at-commit deletes will drop; a trailing zero-`relNumber`
/// `RelFileLocator` terminates the list (the C `MemSet(dest, 0, ...)`). The
/// slice must have room for the survivors plus that terminator.
pub fn SerializePendingSyncs(dest: &mut [RelFileLocator]) -> PgResult<()> {
    PENDING.with(|p| {
        let st = p.borrow();

        let mut written: usize = 0;

        // if (!pendingSyncHash) goto terminate;
        if let Some(sync_hash) = st.sync_hash.as_ref() {
            // tmphash = collect all rlocator from pending syncs
            let mut tmphash: HashMap<RelFileLocator, ()> = HashMap::new();
            tmphash.try_reserve(sync_hash.len()).map_err(|_| oom())?;
            for rlocator in sync_hash.keys() {
                tmphash.insert(*rlocator, ());
            }

            // remove deleted rnodes (delete->atCommit)
            for delete in st.deletes.iter() {
                if delete.at_commit {
                    tmphash.remove(&delete.rlocator);
                }
            }

            // while ((src = hash_seq_search(&scan))) *dest++ = *src;
            for rlocator in tmphash.keys() {
                if written >= dest.len() {
                    return Err(PgError::new(
                        ERROR,
                        "SerializePendingSyncs destination buffer too small",
                    )
                    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
                }
                dest[written] = *rlocator;
                written += 1;
            }
        }

        // terminate: MemSet(dest, 0, sizeof(RelFileLocator));
        if written >= dest.len() {
            return Err(PgError::new(
                ERROR,
                "SerializePendingSyncs destination buffer too small",
            )
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
        }
        dest[written] = RelFileLocator { spcOid: 0, dbOid: 0, relNumber: 0 };
        Ok(())
    })
}

/* ---------------------------------------------------------------------------
 * RestorePendingSyncs (storage.c:651)
 * ------------------------------------------------------------------------- */

/// `RestorePendingSyncs` — restore syncs within a parallel worker.
pub fn RestorePendingSyncs(src: &[RelFileLocator]) -> PgResult<()> {
    // Assert(pendingSyncHash == NULL);
    debug_assert!(PENDING.with(|p| p.borrow().sync_hash.is_none()));

    // for (rlocator = startAddress; rlocator->relNumber != 0; rlocator++)
    //     AddPendingSync(rlocator);
    for rloc in src {
        if rloc.relNumber == 0 {
            break;
        }
        add_pending_sync(*rloc)?;
    }
    Ok(())
}

/* ---------------------------------------------------------------------------
 * smgrDoPendingDeletes (storage.c:673)
 * ------------------------------------------------------------------------- */

/// `smgrDoPendingDeletes` — take care of relation deletes at end of xact.
pub fn smgr_do_pending_deletes(is_commit: bool) -> PgResult<()> {
    let nest_level = xact_seam::get_current_transaction_nest_level::call();

    // Walk pendingDeletes in head->tail order: keep outer-level entries (nest <
    // current), drain all entries at >= nest_level, and of those, smgropen the
    // ones whose atCommit == isCommit.
    let to_delete: Vec<PendingRelDelete> = PENDING.with(|p| {
        let mut st = p.borrow_mut();
        let mut to_delete = Vec::new();
        // entries at >= nest_level matching is_commit are scheduled for delete;
        // ALL entries at >= nest_level are removed from the list.
        for pending in st.deletes.iter() {
            if pending.nest_level >= nest_level && pending.at_commit == is_commit {
                to_delete.push(*pending);
            }
        }
        st.deletes.retain(|pending| pending.nest_level < nest_level);
        to_delete
    });

    // srel = smgropen(...); srels[nrels++] = srel;
    let mut srels: Vec<RelFileLocatorBackend> = Vec::new();
    srels.try_reserve(to_delete.len()).map_err(|_| oom())?;
    for pending in to_delete.iter() {
        let srel = smgr::smgropen(pending.rlocator, pending.proc_number)?;
        srels.push(srel.smgr_rlocator);
    }

    if !srels.is_empty() {
        smgr::smgrdounlinkall(&srels, false)?;
        for srel in &srels {
            smgr::smgrclose(*srel)?;
        }
    }

    Ok(())
}

/* ---------------------------------------------------------------------------
 * smgrDoPendingSyncs (storage.c:741)
 * ------------------------------------------------------------------------- */

/// `smgrDoPendingSyncs` — take care of relation syncs at end of xact.
pub fn smgr_do_pending_syncs(is_commit: bool, is_parallel_worker: bool) -> PgResult<()> {
    // Assert(GetCurrentTransactionNestLevel() == 1);
    debug_assert_eq!(xact_seam::get_current_transaction_nest_level::call(), 1);

    // if (!pendingSyncHash) return;  /* no relation needs sync */
    if PENDING.with(|p| p.borrow().sync_hash.is_none()) {
        return Ok(());
    }

    // Abort -- just throw away all pending syncs.
    if !is_commit {
        PENDING.with(|p| p.borrow_mut().sync_hash = None);
        return Ok(());
    }

    // AssertPendingSyncs_RelationCache() — relcache.c, entirely inside
    // `#ifdef USE_ASSERT_CHECKING` (a no-op in a production build). It opens
    // every relation this transaction has locked to detect a storage/relcache
    // WAL-skip mismatch; it has no production behavior and is not modeled.

    // Parallel worker -- just throw away all pending syncs.
    if is_parallel_worker {
        PENDING.with(|p| p.borrow_mut().sync_hash = None);
        return Ok(());
    }

    // Skip syncing nodes that smgrDoPendingDeletes() will delete; snapshot the
    // surviving sync entries.
    let pending_syncs: Vec<(RelFileLocator, bool)> = PENDING.with(|p| {
        let mut st = p.borrow_mut();
        // remove at-commit deletes from the sync hash.
        let drops: Vec<RelFileLocator> = st
            .deletes
            .iter()
            .filter(|d| d.at_commit)
            .map(|d| d.rlocator)
            .collect();
        if let Some(hash) = st.sync_hash.as_mut() {
            for rlocator in &drops {
                hash.remove(rlocator);
            }
        }
        match st.sync_hash.as_ref() {
            None => Vec::new(),
            Some(hash) => hash.iter().map(|(k, v)| (*k, v.is_truncated)).collect(),
        }
    });

    let wal_skip = wal_skip_threshold();
    let mut srels: Vec<RelFileLocatorBackend> = Vec::new();

    for (rlocator, is_truncated) in pending_syncs.iter().copied() {
        // srel = smgropen(pendingsync->rlocator, INVALID_PROC_NUMBER);
        let srel = smgr::smgropen(rlocator, INVALID_PROC_NUMBER)?;
        let key = srel.smgr_rlocator;

        // nblocks[MAX_FORKNUM + 1]; total_blocks
        let mut nblocks_arr: [BlockNumber; (MAX_FORKNUM_USIZE) + 1] =
            [InvalidBlockNumber; (MAX_FORKNUM_USIZE) + 1];
        let mut total_blocks: u64 = 0;

        if !is_truncated {
            for fork in fork_iter() {
                if smgr::smgrexists(key, fork)? {
                    let n = smgr::smgrnblocks(key, fork)?;
                    // we shouldn't come here for unlogged relations
                    debug_assert_ne!(fork, INIT_FORKNUM);
                    nblocks_arr[fork as usize] = n;
                    total_blocks += n as u64;
                } else {
                    nblocks_arr[fork as usize] = InvalidBlockNumber;
                }
            }
        }

        // Sync file or emit WAL records for its contents.
        if is_truncated || total_blocks >= (wal_skip as u64) * 1024 / (BLCKSZ as u64) {
            srels.try_reserve(1).map_err(|_| oom())?;
            srels.push(key);
        } else {
            // Emit WAL records for all blocks.  The file is small enough.
            for fork in fork_iter() {
                let n = nblocks_arr[fork as usize];
                if !BlockNumberIsValid(n) {
                    continue;
                }
                // rel = CreateFakeRelcacheEntry(...); log_newpage_range(...);
                // FreeFakeRelcacheEntry(rel);
                log_newpage_range_fake(rlocator, fork, n)?;
            }
        }
    }

    // pendingSyncHash = NULL;
    PENDING.with(|p| p.borrow_mut().sync_hash = None);

    if !srels.is_empty() {
        smgr::smgrdosyncall(&srels)?;
    }

    Ok(())
}

/// `rel = CreateFakeRelcacheEntry(locator); log_newpage_range(rel, fork, 0, n,
/// false); FreeFakeRelcacheEntry(rel)`.
fn log_newpage_range_fake(
    rlocator: RelFileLocator,
    fork: ForkNumber,
    n: BlockNumber,
) -> PgResult<()> {
    let ctx = mcx::MemoryContext::new("log_newpage_range_fake");
    let mcx = ctx.mcx();
    // rel = CreateFakeRelcacheEntry(locator);
    let fakerel = xlogutils::CreateFakeRelcacheEntry(mcx, rlocator)?;
    let rel = types_rel::Relation::open(fakerel, None);
    // log_newpage_range(rel, fork, 0, n, false);
    let r = xloginsert::log_newpage_range(&rel, fork, 0, n, false);
    // FreeFakeRelcacheEntry(rel) — the owned value-slice is reclaimed by
    // dropping the carrier (the C smgrclose+pfree); drop forced before `ctx`.
    drop(rel);
    r
}

/// `MAX_FORKNUM` as a `usize` array bound: `INIT_FORKNUM`.
const MAX_FORKNUM_USIZE: usize = INIT_FORKNUM as usize;

/// Iterate `for (fork = 0; fork <= MAX_FORKNUM; fork++)`.
fn fork_iter() -> impl Iterator<Item = ForkNumber> {
    [MAIN_FORKNUM, FSM_FORKNUM, VISIBILITYMAP_FORKNUM, INIT_FORKNUM].into_iter()
}

/* ---------------------------------------------------------------------------
 * smgrGetPendingDeletes (storage.c:893)
 * ------------------------------------------------------------------------- */

/// `smgrGetPendingDeletes` — get the list of non-temp relations to be deleted.
///
/// Only non-temp relations at >= current nesting level with matching `atCommit`
/// are included. Allocated in `mcx` (C: `palloc` into the caller's context).
pub fn smgr_get_pending_deletes<'mcx>(
    mcx: Mcx<'mcx>,
    for_commit: bool,
) -> PgResult<mcx::PgVec<'mcx, RelFileLocator>> {
    let nest_level = xact_seam::get_current_transaction_nest_level::call();

    PENDING.with(|p| {
        let st = p.borrow();
        // Count then fill (the C two-pass).
        let count = st
            .deletes
            .iter()
            .filter(|pending| {
                pending.nest_level >= nest_level
                    && pending.at_commit == for_commit
                    && pending.proc_number == INVALID_PROC_NUMBER
            })
            .count();
        let mut rels = mcx::vec_with_capacity_in(mcx, count)?;
        for pending in st.deletes.iter().filter(|pending| {
            pending.nest_level >= nest_level
                && pending.at_commit == for_commit
                && pending.proc_number == INVALID_PROC_NUMBER
        }) {
            rels.push(pending.rlocator);
        }
        Ok(rels)
    })
}

/* ---------------------------------------------------------------------------
 * DropRelationFiles (md.c:1601) — owned here per the seam contract (isRedo=false)
 * ------------------------------------------------------------------------- */

/// `DropRelationFiles(delrels, ndelrels, isRedo=false)` (md.c): drop the
/// physical files a finished prepared/aborted transaction was supposed to
/// delete. The seam carries only the `false` (non-redo) leg the 2PC / xact
/// consumers use.
pub fn drop_relation_files(rels: &[types_wal::RelFileLocator]) -> PgResult<()> {
    // srels = palloc(sizeof(SMgrRelation) * ndelrels);
    let mut srels: Vec<RelFileLocatorBackend> = Vec::new();
    srels.try_reserve(rels.len()).map_err(|_| oom())?;
    for r in rels {
        // srel = smgropen(delrels[i], INVALID_PROC_NUMBER);  (isRedo=false →
        // no XLogDropRelation loop)  srels[i] = srel;
        let srel = smgr::smgropen(from_wal_locator(*r), INVALID_PROC_NUMBER)?;
        srels.push(srel.smgr_rlocator);
    }

    // smgrdounlinkall(srels, ndelrels, isRedo);
    smgr::smgrdounlinkall(&srels, false)?;

    // for (i...) smgrclose(srels[i]);
    for srel in &srels {
        smgr::smgrclose(*srel)?;
    }
    Ok(())
}

/* ---------------------------------------------------------------------------
 * PostPrepare_smgr (storage.c:934)
 * ------------------------------------------------------------------------- */

/// `PostPrepare_smgr` — clean up after a successful PREPARE.
pub fn post_prepare_smgr() {
    // for (pending = pendingDeletes; ...) { pendingDeletes = next; pfree(pending); }
    PENDING.with(|p| p.borrow_mut().deletes.clear());
}

/* ---------------------------------------------------------------------------
 * AtSubCommit_smgr (storage.c:955)
 * ------------------------------------------------------------------------- */

/// `AtSubCommit_smgr` — reassign pending-deletes to the parent transaction.
pub fn at_subcommit_smgr() {
    let nest_level = xact_seam::get_current_transaction_nest_level::call();
    PENDING.with(|p| {
        let mut st = p.borrow_mut();
        for pending in st.deletes.iter_mut() {
            if pending.nest_level >= nest_level {
                pending.nest_level = nest_level - 1;
            }
        }
    });
}

/* ---------------------------------------------------------------------------
 * AtSubAbort_smgr (storage.c:975)
 * ------------------------------------------------------------------------- */

/// `AtSubAbort_smgr` — delete created relations and forget about deleted ones.
pub fn at_subabort_smgr() -> PgResult<()> {
    smgr_do_pending_deletes(false)
}

/* ---------------------------------------------------------------------------
 * smgr_redo (storage.c:981)
 * ------------------------------------------------------------------------- */

/// `smgr_redo` — `RM_SMGR_ID` rmgr redo routine.
pub fn smgr_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> PgResult<()> {
    // lsn = record->EndRecPtr;
    let lsn = record.EndRecPtr;
    let decoded = record
        .record
        .as_ref()
        .expect("smgr_redo: XLogReaderState has no decoded record");

    // info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    let info = decoded.info() & !XLR_INFO_MASK;
    // Assert(!XLogRecHasAnyBlockRefs(record));
    debug_assert!(decoded.blocks().iter().all(|b| !b.in_use()));
    let data = decoded.data();

    if info == XLOG_SMGR_CREATE {
        // xlrec = (xl_smgr_create *) XLogRecGetData(record);
        let xlrec = types_wal::rmgrdesc::xl_smgr_create::from_bytes(data)
            .ok_or_else(|| PgError::new(PANIC, "smgr_redo: truncated xl_smgr_create"))?;
        // reln = smgropen(xlrec->rlocator, INVALID_PROC_NUMBER);
        let reln = smgr::smgropen(from_wal_locator(xlrec.rlocator()), INVALID_PROC_NUMBER)?;
        // smgrcreate(reln, xlrec->forkNum, true);
        smgr::smgrcreate(reln.smgr_rlocator, xlrec.fork_num(), true)?;
        Ok(())
    } else if info == XLOG_SMGR_TRUNCATE {
        // xlrec = (xl_smgr_truncate *) XLogRecGetData(record);
        let xlrec = types_wal::rmgrdesc::xl_smgr_truncate::from_bytes(data)
            .ok_or_else(|| PgError::new(PANIC, "smgr_redo: truncated xl_smgr_truncate"))?;
        smgr_redo_truncate(lsn, xlrec)
    } else {
        // elog(PANIC, "smgr_redo: unknown op code %u", info);
        Err(PgError::new(
            PANIC,
            format!("smgr_redo: unknown op code {info}"),
        ))
    }
}

/// The `XLOG_SMGR_TRUNCATE` redo body.
fn smgr_redo_truncate(
    lsn: types_core::primitive::XLogRecPtr,
    xlrec: types_wal::rmgrdesc::xl_smgr_truncate,
) -> PgResult<()> {
    let mut need_fsm_vacuum = false;
    let flags = xlrec.flags();
    let rlocator = from_wal_locator(xlrec.rlocator());
    let blkno = xlrec.blkno();

    // reln = smgropen(xlrec->rlocator, INVALID_PROC_NUMBER);
    let reln = smgr::smgropen(rlocator, INVALID_PROC_NUMBER)?;
    let key = reln.smgr_rlocator;

    // Forcibly create relation if it doesn't exist.
    // smgrcreate(reln, MAIN_FORKNUM, true);
    smgr::smgrcreate(key, MAIN_FORKNUM, true)?;

    // Update minimum recovery point to cover this WAL record.  XLogFlush(lsn);
    xlog_seam::xlog_flush::call(lsn)?;

    let mut forks: Vec<ForkNumber> = Vec::new();
    let mut old_blocks: Vec<BlockNumber> = Vec::new();
    let mut blocks: Vec<BlockNumber> = Vec::new();

    // Prepare for truncation of MAIN fork.
    if (flags & SMGR_TRUNCATE_HEAP) != 0 {
        forks.push(MAIN_FORKNUM);
        old_blocks.push(smgr::smgrnblocks(key, MAIN_FORKNUM)?);
        blocks.push(blkno);

        // Also tell xlogutils.c about it.
        xlogutils::XLogTruncateRelation(rlocator, MAIN_FORKNUM, blkno)?;
    }

    // Prepare for truncation of FSM and VM too.
    // rel = CreateFakeRelcacheEntry(xlrec->rlocator);
    {
        let ctx = mcx::MemoryContext::new("smgr_redo truncate");
        let mcx = ctx.mcx();
        let fakerel = xlogutils::CreateFakeRelcacheEntry(mcx, rlocator)?;
        let rel = types_rel::Relation::open(fakerel, None);

        let result = (|| -> PgResult<()> {
            if (flags & SMGR_TRUNCATE_FSM) != 0 && smgr::smgrexists(key, FSM_FORKNUM)? {
                let b = freespace::FreeSpaceMapPrepareTruncateRel(&rel, blkno)?;
                if BlockNumberIsValid(b) {
                    forks.push(FSM_FORKNUM);
                    old_blocks.push(smgr::smgrnblocks(key, FSM_FORKNUM)?);
                    blocks.push(b);
                    need_fsm_vacuum = true;
                }
            }
            if (flags & SMGR_TRUNCATE_VM) != 0 && smgr::smgrexists(key, VISIBILITYMAP_FORKNUM)? {
                let b = visibilitymap::visibilitymap_prepare_truncate(&rel, blkno)?;
                if BlockNumberIsValid(b) {
                    forks.push(VISIBILITYMAP_FORKNUM);
                    old_blocks.push(smgr::smgrnblocks(key, VISIBILITYMAP_FORKNUM)?);
                    blocks.push(b);
                }
            }

            // Do the real work to truncate relation forks.
            if !forks.is_empty() {
                miscinit_seam::start_crit_section::call();
                smgr::smgrtruncate(key, &forks, &old_blocks, &blocks)?;
                miscinit_seam::end_crit_section::call();
            }

            // Update upper-level FSM pages to account for the truncation.
            if need_fsm_vacuum {
                freespace::FreeSpaceMapVacuumRange(&rel, blkno, InvalidBlockNumber)?;
            }
            Ok(())
        })();

        // FreeFakeRelcacheEntry(rel) — reclaim the owned carrier (C
        // smgrclose+pfree).
        drop(rel);
        result
    }
}

/* ---------------------------------------------------------------------------
 * Convenience seams owned by storage.c for the relcache / heapam-handler
 * callers (compose this unit's own functions with smgr / catalog ops).
 * ------------------------------------------------------------------------- */

/// `srel = RelationCreateStorage(newrlocator, persistence, true);
/// smgrclose(srel)` — the non-table-AM `RELKIND_HAS_STORAGE` leg of
/// `RelationSetNewRelfilenumber`.
pub fn relation_create_storage_main_fork(
    newrlocator: RelFileLocator,
    relpersistence: i8,
) -> PgResult<()> {
    let key = RelationCreateStorage(newrlocator, relpersistence, true)?;
    smgr::smgrclose(key)
}

/// The storage-creation leg of `heapam_relation_set_new_filelocator`:
/// `srel = RelationCreateStorage(newrlocator, persistence, true)`, then for an
/// unlogged relation `smgrcreate(srel, INIT_FORKNUM, false)` +
/// `log_smgrcreate(newrlocator, INIT_FORKNUM)`, finally `smgrclose(srel)`.
pub fn relation_set_new_filelocator_storage(
    newrlocator: RelFileLocator,
    relpersistence: i8,
) -> PgResult<()> {
    // srel = RelationCreateStorage(*newrlocator, persistence, true);
    let key = RelationCreateStorage(newrlocator, relpersistence, true)?;

    // if (persistence == RELPERSISTENCE_UNLOGGED) {
    //   smgrcreate(srel, INIT_FORKNUM, false);
    //   log_smgrcreate(newrlocator, INIT_FORKNUM); }
    if relpersistence == RELPERSISTENCE_UNLOGGED {
        smgr::smgrcreate(key, INIT_FORKNUM, false)?;
        log_smgrcreate(newrlocator, INIT_FORKNUM)?;
    }

    // smgrclose(srel);
    smgr::smgrclose(key)
}

/// The init-fork creation leg of `fill_seq_with_data` (sequence.c) for an
/// unlogged sequence: `srel = smgropen(rlocator, INVALID_PROC_NUMBER);
/// smgrcreate(srel, INIT_FORKNUM, false); log_smgrcreate(&rlocator,
/// INIT_FORKNUM)`. The transient `SMgrRelation` is created here; the matching
/// `smgrclose` is done by the caller (after the fork is filled+flushed) through
/// `relation_close_smgr`.
pub fn smgr_create_init_fork_and_log(rlocator: RelFileLocator) -> PgResult<()> {
    // srel = smgropen(rel->rd_locator, INVALID_PROC_NUMBER);
    let srel = smgr::smgropen(rlocator, INVALID_PROC_NUMBER)?;
    let key = srel.smgr_rlocator;
    // smgrcreate(srel, INIT_FORKNUM, false);
    smgr::smgrcreate(key, INIT_FORKNUM, false)?;
    // log_smgrcreate(&rel->rd_locator, INIT_FORKNUM);
    log_smgrcreate(rlocator, INIT_FORKNUM)
}

/// The pg_class-update leg of `RelationSetNewRelfilenumber` (relcache.c:3818-3952)
/// for a non-mapped relation: `table_open(pg_class)`, locked copy of the pg_class
/// row, set `relfilenode = new_relfilenumber` and (for non-sequence relkinds)
/// reset `relpages/reltuples/relallvisible/relallfrozen`, set
/// `relfrozenxid/relminmxid/relpersistence`, then `CatalogTupleUpdate` +
/// `table_close`.
pub fn update_pg_class_relfilenumber(
    relid: Oid,
    new_relfilenumber: Oid,
    relpersistence: i8,
    relkind: i8,
    freeze_xid: u32,
    minmulti: u32,
) -> PgResult<()> {
    let ctx = mcx::MemoryContext::new("update_pg_class_relfilenumber");
    let mcx = ctx.mcx();
    {
        // pg_class = table_open(RelationRelationId, RowExclusiveLock);
        let pg_class = table::table_open(mcx, RelationRelationId, RowExclusiveLock)?;

        // tuple = SearchSysCacheLockedCopy1(RELOID, relid); classform = GETSTRUCT.
        // The value model deforms pg_class into a mutable PgClassForm; the lock /
        // UnlockTuple / heap_freetuple lifecycle is owned by the syscache /
        // indexing owner seams (same path backend-commands-cluster uses).
        let _ = RELOID;
        let Some((otid, mut classform)) =
            syscache_seam::search_syscache_copy_pg_class::call(mcx, relid)?
        else {
            // elog(ERROR, "could not find tuple for relation %u", relid);
            return Err(PgError::new(
                ERROR,
                format!("could not find tuple for relation {relid}"),
            ));
        };

        // classform->relfilenode = newrelfilenumber;
        classform.relfilenode = new_relfilenumber;

        // relpages etc. never change for sequences.
        if relkind != RELKIND_SEQUENCE {
            classform.relpages = 0; // it's empty until further notice
            classform.reltuples = -1.0;
            classform.relallvisible = 0;
            classform.relallfrozen = 0;
        }
        classform.relfrozenxid = freeze_xid;
        classform.relminmxid = minmulti;
        classform.relpersistence = relpersistence as u8;

        // CatalogTupleUpdate(pg_class, &otid, tuple);
        indexing_seam::catalog_tuple_update_pg_class::call(mcx, &pg_class, otid, &classform)?;

        // UnlockTuple + heap_freetuple are folded into the owned-copy lifecycle.
        // table_close(pg_class, RowExclusiveLock);
        table::table_close(pg_class, RowExclusiveLock)?;
        Ok(())
    }
}

/* ---------------------------------------------------------------------------
 * init_seams() — install every seam in backend-catalog-storage-seams.
 * ------------------------------------------------------------------------- */

/// Install every seam this unit owns.
pub fn init_seams() {
    storage_seam::smgr_redo::set(smgr_redo);
    storage_seam::rel_file_locator_skipping_wal::set(rel_file_locator_skipping_wal);
    storage_seam::smgr_do_pending_syncs::set(smgr_do_pending_syncs);
    storage_seam::smgr_do_pending_deletes::set(smgr_do_pending_deletes);
    storage_seam::smgr_get_pending_deletes::set(smgr_get_pending_deletes);
    storage_seam::at_subcommit_smgr::set(at_subcommit_smgr);
    storage_seam::at_subabort_smgr::set(at_subabort_smgr);
    storage_seam::post_prepare_smgr::set(post_prepare_smgr);
    storage_seam::drop_relation_files::set(drop_relation_files);
    storage_seam::relation_drop_storage::set(relation_drop_storage);
    storage_seam::smgr_unlink_relation_now::set(smgr_unlink_relation_now);
    storage_seam::relation_create_storage_main_fork::set(relation_create_storage_main_fork);
    storage_seam::update_pg_class_relfilenumber::set(update_pg_class_relfilenumber);
    storage_seam::relation_preserve_storage::set(RelationPreserveStorage);
    storage_seam::relation_set_new_filelocator_storage::set(relation_set_new_filelocator_storage);
    storage_seam::smgr_create_init_fork_and_log::set(smgr_create_init_fork_and_log);

    // Parallel-worker transfer of pending syncs. The bodies are owned here; the
    // seam decls live in parallel-rt-seams. The DSM chunk is a packed array of
    // `RelFileLocator` records (three `Oid`s = `RelFileLocatorWireSize` bytes
    // each) terminated by an all-zero `relNumber` record, so it is
    // self-delimiting on the restore side.
    {
        use backend_access_transam_parallel_rt_seams as rt;
        rt::estimate_pending_syncs_space::set(EstimatePendingSyncsSpace);
        rt::serialize_pending_syncs::set(|len, space| {
            // The owner writes survivors + a zero terminator into a
            // `RelFileLocator` slice; marshal that into the `len`-byte DSM chunk
            // as packed 12-byte records. SAFETY: `space` is the start of the
            // `len`-byte chunk shm_toc_allocate reserved for pending syncs
            // (EstimatePendingSyncsSpace sized it).
            let records = len / RelFileLocatorWireSize;
            let mut slots = vec![RelFileLocator { spcOid: 0, dbOid: 0, relNumber: 0 }; records];
            SerializePendingSyncs(&mut slots)?;
            let buf = unsafe { core::slice::from_raw_parts_mut(space as *mut u8, len) };
            for (i, rloc) in slots.iter().enumerate() {
                let off = i * RelFileLocatorWireSize;
                buf[off..off + 4].copy_from_slice(&rloc.spc_oid().to_ne_bytes());
                buf[off + 4..off + 8].copy_from_slice(&rloc.db_oid().to_ne_bytes());
                buf[off + 8..off + 12].copy_from_slice(&rloc.rel_number().to_ne_bytes());
            }
            Ok(())
        });
        rt::restore_pending_syncs::set(|space| {
            // Decode packed `RelFileLocator` records up to (and including) the
            // zero-`relNumber` terminator, then hand the slice to the owner.
            // SAFETY: `space` points at the pending-syncs chunk the leader
            // serialized; the terminator bounds the read (the chunk was sized to
            // hold survivors + terminator). We read one record at a time so the
            // first decode failure (short buffer) stops before the terminator.
            let mut decoded: Vec<RelFileLocator> = Vec::new();
            let mut off = 0usize;
            loop {
                let bytes = unsafe {
                    core::slice::from_raw_parts(
                        (space + off) as *const u8,
                        RelFileLocatorWireSize,
                    )
                };
                let rloc = RelFileLocator::from_bytes(bytes)
                    .expect("RelFileLocatorWireSize-byte record");
                decoded.push(rloc);
                off += RelFileLocatorWireSize;
                if rloc.rel_number() == 0 {
                    break;
                }
            }
            RestorePendingSyncs(&decoded)
        });
    }

    // `int wal_skip_threshold = 2048;` (storage.c). Plain backend-local GUC int
    // read directly from its variable at runtime (RelationNeedsWAL/
    // smgrDoPendingSyncs compare the relation size in KB against
    // wal_skip_threshold); it is NOT sourced from the ControlFile. The GUC
    // engine seeds it from boot_val and drives reads/writes through these
    // accessors backed by the owner-local storage above.
    {
        use backend_utils_misc_guc_tables::{vars, GucVarAccessors};
        vars::wal_skip_threshold.install(GucVarAccessors {
            get: wal_skip_threshold,
            set: set_wal_skip_threshold,
        });
    }
}
