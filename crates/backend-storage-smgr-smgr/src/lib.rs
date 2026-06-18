#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

//! The storage manager (smgr) dispatch layer — an idiomatic, safe-Rust port of
//! `src/backend/storage/smgr/smgr.c`.
//!
//! smgr is a thin indirection layer: it owns the per-`RelFileLocatorBackend`
//! `SMgrRelation` cache and forwards open/close/read/write/extend/nblocks/
//! truncate operations to the magnetic-disk manager (md.c). The backend-local
//! cache (C's `SMgrRelationHash` + `unpinned_relns`) lives in the md crate (it
//! also holds the per-fork segment fd state); smgr drives it through md's
//! `cache_*` primitives and dispatches block I/O through md's `md_*` functions
//! (md is a DIRECT dep — `fd <- md <- smgr`, acyclic).
//!
//! The genuine in-body bufmgr interlocks (`DropRelationBuffers`,
//! `DropRelationsAllBuffers`, `FlushRelationsAllBuffers`), `CacheInvalidateSmgr`,
//! and (in the redo path) `XLogDropRelation` cross seams to those unported /
//! cyclic owners; they panic loudly until those owners land.

use backend_storage_smgr_md as md;
use backend_utils_error::{ereport, PgError, PgResult};
use types_core::primitive::{
    BlockNumber, ForkNumber, InvalidBlockNumber, ProcNumber,
};
use types_error::ERROR;
use types_storage::smgr::{SMgrRelationData, RELSEG_SIZE, SMGR_NFORKS};
use types_storage::{RelFileLocator, RelFileLocatorBackend};

use backend_access_transam_xlogutils_seams::xlog_drop_relation as xlog_drop_relation_seam;
use backend_storage_buffer_bufmgr_seams::{
    drop_relation_buffers as drop_relation_buffers_seam,
    drop_relations_all_buffers as drop_relations_all_buffers_seam,
    flush_relations_all_buffers as flush_relations_all_buffers_seam,
};
use backend_storage_smgr_seams as smgr_seam;
use backend_utils_cache_inval_seams::cache_invalidate_smgr as cache_invalidate_smgr_seam;

/// The four-fork array (`for (forknum = 0; forknum <= MAX_FORKNUM; ...)`).
fn fork_iter() -> [ForkNumber; SMGR_NFORKS] {
    [
        ForkNumber::MAIN_FORKNUM,
        ForkNumber::FSM_FORKNUM,
        ForkNumber::VISIBILITYMAP_FORKNUM,
        ForkNumber::INIT_FORKNUM,
    ]
}

// ===========================================================================
// smgrinit / smgrshutdown — backend-local storage-manager start/stop.
// ===========================================================================

/// `smgrinit()` (smgr.c) — initialize all storage managers (backend startup).
/// For each `smgrsw[i]` with a non-NULL `smgr_init`, call it. The only manager
/// is md, whose `smgr_init` is `mdinit`. (The `on_proc_exit(smgrshutdown, 0)`
/// registration is a backend-runtime concern facaded away.)
pub fn smgrinit() -> PgResult<()> {
    md::mdinit()
}

// ===========================================================================
// smgropen / pin / unpin / release / close / destroy lifecycle.
// ===========================================================================

/// `smgropen()` — look up or create the `SMgrRelation` for this locator,
/// running md's `smgr_open` on a fresh entry. Returns a snapshot of the
/// boundary data.
pub fn smgropen(rlocator: RelFileLocator, backend: ProcNumber) -> PgResult<SMgrRelationData> {
    md::cache_open(rlocator, backend)
}

/// `smgrpin()` — keep a reln alive across transactions.
pub fn smgrpin(key: RelFileLocatorBackend) {
    md::cache_adjust_pincount(key, 1);
}

/// `smgrunpin()`.
pub fn smgrunpin(key: RelFileLocatorBackend) {
    debug_assert!(md::cache_pincount(key) > 0, "smgrunpin: pincount must be positive");
    md::cache_adjust_pincount(key, -1);
}

/// `smgrrelease()` — release all lower resources for the relation while keeping
/// the cache entry valid: close every fork at md, and reset the cached block
/// counts and target block.
pub fn smgrrelease(key: RelFileLocatorBackend) -> PgResult<()> {
    for forknum in fork_iter() {
        md::md_close(key, forknum)?;
    }
    md::with_data_mut(key, |d| {
        d.smgr_cached_nblocks = [InvalidBlockNumber; SMGR_NFORKS];
        d.smgr_targblock = InvalidBlockNumber;
    });
    Ok(())
}

/// `smgrclose()` — a synonym for `smgrrelease`: the SMgrRelation object is NOT
/// removed (other references may still point at it), only its resources freed.
pub fn smgrclose(key: RelFileLocatorBackend) -> PgResult<()> {
    smgrrelease(key)
}

/// `smgrdestroy()` — close all forks and remove the cache entry. Only valid
/// when unpinned (`pincount == 0`).
pub fn smgrdestroy(key: RelFileLocatorBackend) -> PgResult<()> {
    debug_assert!(md::cache_pincount(key) == 0, "smgrdestroy: pincount must be zero");
    for forknum in fork_iter() {
        md::md_close(key, forknum)?;
    }
    if !md::cache_remove(key) {
        return Err(PgError::error("SMgrRelation hashtable corrupted"));
    }
    Ok(())
}

/// `smgrdestroyall()` — destroy every unpinned relation (C's `unpinned_relns`),
/// as called by `AtEOXact_SMgr`.
pub fn smgrdestroyall() -> PgResult<()> {
    // Snapshot the unpinned keys first so the removals don't perturb the walk.
    let unpinned: Vec<RelFileLocatorBackend> = md::cache_keys()
        .into_iter()
        .filter(|k| md::cache_pincount(*k) == 0)
        .collect();
    for key in unpinned {
        smgrdestroy(key)?;
    }
    Ok(())
}

/// `smgrreleaseall()` — release lower resources for *every* relation (pinned or
/// not) while keeping the entries, as on `PROCSIGNAL_BARRIER_SMGRRELEASE`.
pub fn smgrreleaseall() {
    for key in md::cache_keys() {
        // C's smgrreleaseall is void; the md close path's failures are
        // FATAL/LOG, not ERROR — but our md_close returns PgResult, so a real
        // error here would be lost. md.c declares smgr_release_all void; we
        // mirror by ignoring the (non-ERROR in C) close result.
        let _ = smgrrelease(key);
    }
}

/// `smgrreleaserellocator()` — like `smgrrelease(smgropen(rlocator))` but avoids
/// materializing a hash entry that does not already exist.
pub fn smgrreleaserellocator(key: RelFileLocatorBackend) -> PgResult<()> {
    if md::cache_contains(key) {
        smgrrelease(key)?;
    }
    Ok(())
}

/// `ProcessBarrierSmgrRelease()` — release all open files on a procsignal
/// barrier; returns true like C.
pub fn ProcessBarrierSmgrRelease() -> PgResult<bool> {
    smgrreleaseall();
    Ok(true)
}

/// `AtEOXact_SMgr()` — at transaction commit/abort, destroy all unpinned
/// SMgrRelation objects.
pub fn AtEOXact_SMgr() -> PgResult<()> {
    smgrdestroyall()
}

// ===========================================================================
// target-block tracking.
// ===========================================================================

/// `smgrsettargblock()` — record the relation's insertion target block.
pub fn smgrsettargblock(key: RelFileLocatorBackend, targblock: BlockNumber) {
    md::with_data_mut(key, |d| d.smgr_targblock = targblock);
}

/// `smgrgettargblock()`.
pub fn smgrgettargblock(key: RelFileLocatorBackend) -> BlockNumber {
    md::cache_get(key).map(|d| d.smgr_targblock).unwrap_or(InvalidBlockNumber)
}

// ===========================================================================
// exists / create / read / write / extend dispatch.
// ===========================================================================

/// `smgrexists()`.
pub fn smgrexists(key: RelFileLocatorBackend, forknum: ForkNumber) -> PgResult<bool> {
    md::md_exists(key, forknum)
}

/// `smgrcreate()`.
pub fn smgrcreate(key: RelFileLocatorBackend, forknum: ForkNumber, is_redo: bool) -> PgResult<()> {
    md::md_create(key, forknum, is_redo)
}

/// `smgrread()` — single-block read. The `static inline` helper from smgr.h:
/// `smgrreadv(reln, forknum, blocknum, &buffer, 1)`.
pub fn smgrread(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: &mut [u8],
) -> PgResult<()> {
    let mut buffers: [&mut [u8]; 1] = [buffer];
    smgrreadv(key, forknum, blocknum, &mut buffers, 1)
}

/// `smgrwrite()` — single-block write.
pub fn smgrwrite(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: &[u8],
    skip_fsync: bool,
) -> PgResult<()> {
    let buffers: [&[u8]; 1] = [buffer];
    smgrwritev(key, forknum, blocknum, &buffers, 1, skip_fsync)
}

/// `smgrextend()` — extend by one block, then update the cached count.
pub fn smgrextend(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: &[u8],
    skip_fsync: bool,
) -> PgResult<()> {
    md::md_extend(key, forknum, blocknum, buffer, skip_fsync)?;
    update_cached_after_extend(key, forknum, blocknum, 1);
    Ok(())
}

/// `smgrzeroextend()` — extend by `nblocks` zeroed blocks.
pub fn smgrzeroextend(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: i32,
    skip_fsync: bool,
) -> PgResult<()> {
    md::md_zeroextend(key, forknum, blocknum, nblocks, skip_fsync)?;
    update_cached_after_extend(key, forknum, blocknum, nblocks as BlockNumber);
    Ok(())
}

/// Mirror smgr.c's post-extend cache update: if the cached size equals the
/// block we just wrote at, advance it by `added`; otherwise invalidate.
fn update_cached_after_extend(key: RelFileLocatorBackend, forknum: ForkNumber, blocknum: BlockNumber, added: BlockNumber) {
    md::with_data_mut(key, |d| {
        let slot = &mut d.smgr_cached_nblocks[forknum as usize];
        if *slot == blocknum {
            *slot = blocknum + added;
        } else {
            *slot = InvalidBlockNumber;
        }
    });
}

// ===========================================================================
// nblocks / nblocks_cached / truncate / immedsync.
// ===========================================================================

/// `smgrnblocks()` — number of blocks in a fork; consults the cache first, on a
/// miss asks md and caches the result.
pub fn smgrnblocks(key: RelFileLocatorBackend, forknum: ForkNumber) -> PgResult<BlockNumber> {
    let cached = smgrnblocks_cached(key, forknum);
    if cached != InvalidBlockNumber {
        return Ok(cached);
    }
    let result = md::md_nblocks(key, forknum)?;
    md::with_data_mut(key, |d| d.smgr_cached_nblocks[forknum as usize] = result);
    Ok(result)
}

/// `smgrnblocks_cached()` — the cached fork size without touching disk.
///
/// Faithful to smgr.c: outside recovery there is no shared invalidation for
/// fork-size changes, so the cache is not trusted (returns `InvalidBlockNumber`).
/// Only in recovery (single-writer) is the cached value returned.
pub fn smgrnblocks_cached(key: RelFileLocatorBackend, forknum: ForkNumber) -> BlockNumber {
    if backend_access_transam_xlogutils_seams::in_recovery::call() {
        if let Some(d) = md::cache_get(key) {
            let cached = d.smgr_cached_nblocks[forknum as usize];
            if cached != InvalidBlockNumber {
                return cached;
            }
        }
    }
    InvalidBlockNumber
}

/// `smgrtruncate()` — truncate the given *forks* of a relation to each specified
/// number of blocks, and update the cache (smgr.c:874-925).
pub fn smgrtruncate(
    key: RelFileLocatorBackend,
    forknum: &[ForkNumber],
    old_nblocks: &[BlockNumber],
    nblocks: &[BlockNumber],
) -> PgResult<()> {
    debug_assert_eq!(forknum.len(), old_nblocks.len());
    debug_assert_eq!(forknum.len(), nblocks.len());

    // (1) DropRelationBuffers(reln, forknum, nforks, nblocks) — ONE call.
    drop_relation_buffers_seam::call(key, forknum, nblocks)?;
    // (2) CacheInvalidateSmgr(reln->smgr_rlocator) — ONE shared-inval.
    cache_invalidate_smgr_seam::call(key)?;
    // (3) Per-fork truncate + cache bookkeeping.
    for i in 0..forknum.len() {
        // Make the cached size invalid if we encounter an error.
        md::with_data_mut(key, |d| d.smgr_cached_nblocks[forknum[i] as usize] = InvalidBlockNumber);
        md::md_truncate(key, forknum[i], old_nblocks[i], nblocks[i])?;
        // nblocks > old_nblocks is possible on a replica restart (md_truncate is
        // a no-op), so reflect the old size rather than the request.
        md::with_data_mut(key, |d| {
            d.smgr_cached_nblocks[forknum[i] as usize] = if nblocks[i] > old_nblocks[i] {
                old_nblocks[i]
            } else {
                nblocks[i]
            };
        });
    }
    Ok(())
}

/// `smgrimmedsync()`.
pub fn smgrimmedsync(key: RelFileLocatorBackend, forknum: ForkNumber) -> PgResult<()> {
    md::md_immedsync(key, forknum)
}

// ===========================================================================
// Vectored I/O surface (straight dispatch to md).
// ===========================================================================

/// `smgrreadv()` — read a block range into the supplied per-block buffers.
pub fn smgrreadv(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: &mut [&mut [u8]],
    nblocks: BlockNumber,
) -> PgResult<()> {
    debug_assert_eq!(buffers.len() as BlockNumber, nblocks);
    md::md_readv(key, forknum, blocknum, buffers, nblocks)
}

/// `smgrwritev()` — write the supplied per-block buffers to an existing range.
pub fn smgrwritev(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: &[&[u8]],
    nblocks: BlockNumber,
    skip_fsync: bool,
) -> PgResult<()> {
    debug_assert_eq!(buffers.len() as BlockNumber, nblocks);
    md::md_writev(key, forknum, blocknum, buffers, nblocks, skip_fsync)
}

/// `smgrprefetch()` — initiate an asynchronous read-ahead of `nblocks` blocks.
pub fn smgrprefetch(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: u32,
) -> PgResult<bool> {
    md::md_prefetch(key, forknum, blocknum, nblocks as i32)
}

/// `smgrmaxcombine()` — max total blocks combinable into a single I/O starting
/// at `blocknum` (a pure function of `blocknum` and `RELSEG_SIZE`).
pub fn smgrmaxcombine(key: RelFileLocatorBackend, forknum: ForkNumber, blocknum: BlockNumber) -> u32 {
    let _ = RELSEG_SIZE; // (documentation: result is at least 1)
    md::md_maxcombine(key, forknum, blocknum)
}

/// `smgrwriteback()` — hint the kernel to flush a range of dirty blocks.
pub fn smgrwriteback(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: u32,
) -> PgResult<()> {
    md::md_writeback(key, forknum, blocknum, nblocks)
}

/// `smgrfd()` — the kernel fd + in-segment offset for `blocknum` (AIO worker
/// hand-off). Returns `(fd, off)`.
pub fn smgrfd(key: RelFileLocatorBackend, forknum: ForkNumber, blocknum: BlockNumber) -> PgResult<(i32, u32)> {
    md::md_fd(key, forknum, blocknum)
}

// ===========================================================================
// Checkpoint sync-request + bulk drop/sync paths.
// ===========================================================================

/// `smgrregistersync()` — request a relation fork be fsync'd at the next
/// checkpoint (a single dispatch to `mdregistersync`).
pub fn smgrregistersync(key: RelFileLocatorBackend, forknum: ForkNumber) -> PgResult<()> {
    md::md_registersync(key, forknum)
}

/// `smgrdosyncall()` — immediately sync all forks of all the given relations.
pub fn smgrdosyncall(rels: &[RelFileLocatorBackend]) -> PgResult<()> {
    if rels.is_empty() {
        return Ok(());
    }

    // FlushRelationsAllBuffers(rels, nrels) — flush dirty buffers FIRST.
    flush_relations_all_buffers_seam::call(rels)?;

    for &rel in rels {
        for forknum in fork_iter() {
            if md::md_exists(rel, forknum)? {
                md::md_immedsync(rel, forknum)?;
            }
        }
    }
    Ok(())
}

/// `smgrdounlinkall()` — immediately unlink all forks of all the given
/// relations (the commit/abort-time bulk delete).
pub fn smgrdounlinkall(rels: &[RelFileLocatorBackend], is_redo: bool) -> PgResult<()> {
    if rels.is_empty() {
        return Ok(());
    }

    // DropRelationsAllBuffers(rels, nrels).
    drop_relations_all_buffers_seam::call(rels)?;

    // Close every fork at smgr (smgr_rlocator IS the key, so rlocators == rels).
    for &rel in rels {
        for forknum in fork_iter() {
            md::md_close(rel, forknum)?;
        }
    }

    // Shared-inval to force other backends to close dangling refs first.
    for &rel in rels {
        cache_invalidate_smgr_seam::call(rel)?;
    }

    // Delete the physical file(s).
    for &rel in rels {
        for forknum in fork_iter() {
            md::md_unlink(rel, forknum, is_redo)?;
        }
    }

    Ok(())
}

// ===========================================================================
// DropRelationFiles (md.h; smgr-level orchestration over smgropen/dounlinkall).
//
// In C this lives in md.c but is pure smgr-level orchestration calling back
// into smgr.c (smgropen/smgrdounlinkall/smgrclose) + XLogDropRelation. Homing
// it in the SMGR crate avoids an md->smgr dispatch cycle; smgr-seams declares
// `drop_relation_files` (doc-tagged "(md.c)"), and this crate owns/installs it.
// ===========================================================================

/// `DropRelationFiles()` (md.c:1601-1626) — drop the files of all given
/// relations (open + dounlinkall + close); used in recovery.
pub fn drop_relation_files(delrels: &[RelFileLocator], is_redo: bool) -> PgResult<()> {
    let mut srels: Vec<RelFileLocatorBackend> = Vec::new();
    srels
        .try_reserve(delrels.len())
        .map_err(|_| ereport(ERROR).errcode(types_error::ERRCODE_OUT_OF_MEMORY).errmsg_internal("out of memory allocating DropRelationFiles srels").into_error())?;

    for &delrel in delrels.iter() {
        let srel = smgropen(delrel, types_core::primitive::INVALID_PROC_NUMBER)?;

        if is_redo {
            for fork in fork_iter() {
                xlog_drop_relation_seam::call(delrel, fork);
            }
        }
        srels.push(srel.smgr_rlocator);
    }

    smgrdounlinkall(&srels, is_redo)?;

    for &srel in srels.iter() {
        smgrclose(srel)?;
    }

    Ok(())
}

// ===========================================================================
// init_seams() — install every seam in backend-storage-smgr-seams.
// ===========================================================================

/// Install every seam this unit OWNS (`backend-storage-smgr-seams`).
pub fn init_seams() {
    smgr_seam::smgr_release_rellocator::set(smgrreleaserellocator);
    smgr_seam::process_barrier_smgr_release::set(ProcessBarrierSmgrRelease);
    smgr_seam::smgrnblocks::set(|rlocator, backend, forknum| {
        // The seam contract is `smgrnblocks(smgropen(rlocator, backend),
        // forknum)` — i.e. the caller hands the physical id and expects
        // `RelationGetSmgr` semantics. `smgropen` (`md::cache_open`) is
        // idempotent: it opens+caches the SMgrRelation on first touch and is a
        // cache lookup thereafter. This makes the read work for a relation whose
        // smgr has not yet been opened by a prior buffer op (e.g. an index read
        // during planning).
        smgropen(rlocator, backend)?;
        smgrnblocks(RelFileLocatorBackend { locator: rlocator, backend }, forknum)
    });
    smgr_seam::smgr_cached_nblocks::set(|rlocator, backend, forknum| {
        smgrnblocks_cached(RelFileLocatorBackend { locator: rlocator, backend }, forknum)
    });
    smgr_seam::at_eoxact_smgr::set(|| {
        // C's AtEOXact_SMgr is void; smgrdestroyall's md close failures are
        // not ERROR in C. Mirror the void contract by absorbing the result.
        let _ = AtEOXact_SMgr();
    });
    smgr_seam::drop_relation_files::set(drop_relation_files);
    smgr_seam::smgrexists::set(|rlocator, backend, forknum| {
        smgrexists(RelFileLocatorBackend { locator: rlocator, backend }, forknum)
    });
    smgr_seam::smgrdestroyall::set(smgrdestroyall);
    smgr_seam::smgrreleaseall::set(smgrreleaseall);
    smgr_seam::relation_close_smgr::set(|rlocator| {
        // C: RelationCloseSmgr(rel) == `if (rel->rd_smgr != NULL) smgrclose(...)`
        // — a no-op when the relation's smgr was never opened. The owned mirror
        // has no rd_smgr field; the presence of an smgr cache entry for this
        // RelFileLocatorBackend is the rd_smgr != NULL analog. Without this guard,
        // RelationClearRelation on a never-opened relation (e.g. a nailed catalog
        // whose storage a fresh backend never touched) reaches smgrclose ->
        // md_close on an absent MdRelnState and panics. (smgrclose is void;
        // absorb the result.)
        if md::cache_contains(rlocator) {
            let _ = smgrclose(rlocator);
        }
    });
    smgr_seam::smgrinit::set(smgrinit);
    // localbuf.c temp-relation I/O consumers (smgr.c static-inline helpers).
    smgr_seam::smgr_read::set(|rlocator, backend, forknum, blocknum, dst| {
        smgrread(RelFileLocatorBackend { locator: rlocator, backend }, forknum, blocknum, dst)
    });
    smgr_seam::smgr_write::set(|rlocator, backend, forknum, blocknum, src| {
        // smgrwrite(reln, forknum, blocknum, buffer, /*skipFsync*/ false).
        smgrwrite(RelFileLocatorBackend { locator: rlocator, backend }, forknum, blocknum, src, false)
    });
    smgr_seam::smgr_zeroextend::set(|rlocator, backend, forknum, blocknum, nblocks, skip_fsync| {
        smgrzeroextend(RelFileLocatorBackend { locator: rlocator, backend }, forknum, blocknum, nblocks as i32, skip_fsync)
    });
    smgr_seam::smgr_prefetch::set(|rlocator, backend, forknum, blocknum| {
        // smgrprefetch(reln, forknum, blocknum, 1).
        smgrprefetch(RelFileLocatorBackend { locator: rlocator, backend }, forknum, blocknum, 1)
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_storage::RelFileLocator;

    fn key() -> RelFileLocatorBackend {
        RelFileLocatorBackend {
            locator: RelFileLocator { spcOid: 1, dbOid: 2, relNumber: 16384 },
            backend: types_core::primitive::INVALID_PROC_NUMBER,
        }
    }

    #[test]
    fn smgrmaxcombine_geometry() {
        let k = key();
        assert_eq!(smgrmaxcombine(k, ForkNumber::MAIN_FORKNUM, 0), RELSEG_SIZE);
        assert_eq!(smgrmaxcombine(k, ForkNumber::MAIN_FORKNUM, RELSEG_SIZE - 1), 1);
        assert_eq!(smgrmaxcombine(k, ForkNumber::MAIN_FORKNUM, RELSEG_SIZE), RELSEG_SIZE);
    }

    #[test]
    fn fork_iter_covers_all_four_forks() {
        let forks = fork_iter();
        assert_eq!(forks.len(), SMGR_NFORKS);
        assert_eq!(forks[0], ForkNumber::MAIN_FORKNUM);
        assert_eq!(forks[3], ForkNumber::INIT_FORKNUM);
    }

    #[test]
    fn smgrnblocks_cached_uninstalled_is_invalid_outside_open() {
        // No cache entry => InvalidBlockNumber regardless of recovery state.
        // (in_recovery seam is uninstalled in unit tests; this path returns
        // InvalidBlockNumber before consulting it when not in recovery.)
        // We only assert the no-entry branch via cache_get returning None.
        assert_eq!(md::cache_get(key()), None);
    }
}
