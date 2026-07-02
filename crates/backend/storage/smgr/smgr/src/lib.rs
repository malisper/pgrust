#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! src/backend/storage/smgr/smgr.c — dispatch layer + the backend-private
//! `SMgrRelation` handle cache keyed by `RelFileLocatorBackend`.

use core::cell::{Cell, RefCell};

use ::elog::ereport;
use ::mcx::{MemoryContext, Mcx, PgHashMap, PgVec};
use ::types_core::primitive::{
    BlockNumber, ForkNumber, InvalidBlockNumber, ProcNumber, INVALID_PROC_NUMBER,
};
use ::types_error::{PgError, PgResult, ERRCODE_OUT_OF_MEMORY, ERROR};
use ::types_storage::file::File;
use ::types_storage::smgr::{MdRelnState, SMGR_NFORKS};
use ::types_storage::sync::{FileTag, FileTagOpResult};
use ::types_storage::{RelFileLocator, RelFileLocatorBackend};

// smgrsw[]: closed set; a new manager is a new variant (rule 4).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SmgrKind {
    Md,
}

pub struct SMgrRelation {
    pub smgr_targblock: BlockNumber,
    pub smgr_cached_nblocks: [BlockNumber; SMGR_NFORKS],
    which: SmgrKind,
    pincount: i32,
    // C's `rd_smgr != NULL`: keeps the relcache's single pin idempotent.
    relcache_pinned: bool,
    md: MdRelnState,
}

const _: () = assert!(core::mem::size_of::<SMgrRelation>() <= 152);

impl SMgrRelation {
    fn new() -> Self {
        SMgrRelation {
            smgr_targblock: InvalidBlockNumber,
            smgr_cached_nblocks: [InvalidBlockNumber; SMGR_NFORKS],
            which: SmgrKind::Md,
            pincount: 0,
            relcache_pinned: false,
            md: MdRelnState::default(),
        }
    }
}

struct SmgrCache {
    cx: &'static MemoryContext,
    relns: PgHashMap<'static, RelFileLocatorBackend, SMgrRelation>,
    // |unpinned_relns|: AtEOXact_SMgr skips the walk when all are pinned.
    unpinned: Cell<usize>,
}

thread_local! {
    static CACHE: RefCell<Option<SmgrCache>> = const { RefCell::new(None) };
}

#[cold]
#[inline(never)]
fn oom(what: &str) -> Box<PgError> {
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_OUT_OF_MEMORY)
            .errmsg_internal(format!("out of memory allocating {what}"))
            .into_error(),
    )
}

fn with_cache<R>(f: impl FnOnce(&mut SmgrCache) -> R) -> R {
    CACHE.with(|c| {
        let mut slot = c.borrow_mut();
        let cache = slot.get_or_insert_with(|| {
            let cx: &'static MemoryContext =
                Box::leak(Box::new(MemoryContext::new("smgr relation table")));
            let mut relns = PgHashMap::new_in(cx.mcx());
            let _ = relns.try_reserve(400);
            SmgrCache { cx, relns, unpinned: Cell::new(0) }
        });
        f(cache)
    })
}

fn scratch_mcx() -> Mcx<'static> {
    with_cache(|c| c.cx.mcx())
}

fn with_reln<R>(key: RelFileLocatorBackend, f: impl FnOnce(&mut SMgrRelation) -> R) -> Option<R> {
    with_cache(|c| c.relns.get_mut(&key).map(f))
}

#[track_caller]
fn reln<R>(key: RelFileLocatorBackend, f: impl FnOnce(&mut SMgrRelation) -> R) -> R {
    with_reln(key, f).expect("smgr operation on an unopened SMgrRelation")
}

// smgropen's dynahash HASH_ENTER "found" arm miss path: entry creation is
// off the warm-hit path (C's found=false branch), including the capacity
// reservation the fallible-insert contract needs.
#[cold]
#[inline(never)]
fn open_entry<'c>(
    c: &'c mut SmgrCache,
    key: RelFileLocatorBackend,
) -> PgResult<&'c mut SMgrRelation> {
    if c.relns.len() == c.relns.capacity() && c.relns.try_reserve(1).is_err() {
        return Err(oom("SMgrRelation hashtable"));
    }
    let r = c.relns.entry(key).or_insert_with(|| {
        let mut r = SMgrRelation::new();
        match r.which {
            SmgrKind::Md => md::mdopen(&mut r.md),
        }
        r
    });
    c.unpinned.set(c.unpinned.get() + 1);
    Ok(r)
}

fn opened<R>(
    key: RelFileLocatorBackend,
    f: impl FnOnce(&mut SMgrRelation) -> R,
) -> PgResult<R> {
    debug_assert!(key.locator.relNumber != 0, "smgropen: invalid RelFileNumber");
    with_cache(|c| {
        // Warm hit = ONE probe (C smgropen's HASH_ENTER-found), no capacity
        // bookkeeping.
        if let Some(r) = c.relns.get_mut(&key) {
            return Ok(f(r));
        }
        open_entry(c, key).map(f)
    })
}

#[inline]
fn note_pin_transition(unpinned: &Cell<usize>, old: i32, new: i32) {
    if old == 0 && new != 0 {
        unpinned.set(unpinned.get() - 1);
    } else if old != 0 && new == 0 {
        unpinned.set(unpinned.get() + 1);
    }
}

pub fn smgrinit() -> PgResult<()> {
    md::mdinit()
}

pub fn smgropen(rlocator: RelFileLocator, backend: ProcNumber) -> PgResult<()> {
    let key = RelFileLocatorBackend { locator: rlocator, backend };
    opened(key, |_| ())
}

pub fn smgrpin(key: RelFileLocatorBackend) {
    with_cache(|c| {
        if let Some(r) = c.relns.get_mut(&key) {
            let old = r.pincount;
            r.pincount += 1;
            note_pin_transition(&c.unpinned, old, r.pincount);
        }
    });
}

pub fn smgrunpin(key: RelFileLocatorBackend) {
    with_cache(|c| {
        if let Some(r) = c.relns.get_mut(&key) {
            debug_assert!(r.pincount > 0, "smgrunpin: pincount must be positive");
            let old = r.pincount;
            r.pincount -= 1;
            note_pin_transition(&c.unpinned, old, r.pincount);
        }
    });
}

pub fn smgrpin_relcache(key: RelFileLocatorBackend) {
    with_cache(|c| {
        if let Some(r) = c.relns.get_mut(&key) {
            if !r.relcache_pinned {
                r.relcache_pinned = true;
                let old = r.pincount;
                r.pincount += 1;
                note_pin_transition(&c.unpinned, old, r.pincount);
            }
        }
    });
}

pub fn smgrunpin_relcache(key: RelFileLocatorBackend) {
    with_cache(|c| {
        if let Some(r) = c.relns.get_mut(&key) {
            if r.relcache_pinned {
                r.relcache_pinned = false;
                let old = r.pincount;
                r.pincount -= 1;
                note_pin_transition(&c.unpinned, old, r.pincount);
            }
        }
    });
}

pub fn smgrrelease(key: RelFileLocatorBackend) -> PgResult<()> {
    reln(key, |r| {
        for forknum in md::fork_iter() {
            match r.which {
                SmgrKind::Md => md::mdclose(&mut r.md, forknum)?,
            }
        }
        r.smgr_cached_nblocks = [InvalidBlockNumber; SMGR_NFORKS];
        r.smgr_targblock = InvalidBlockNumber;
        Ok(())
    })
}

pub fn smgrclose(key: RelFileLocatorBackend) -> PgResult<()> {
    smgrrelease(key)
}

pub fn smgrdestroy(key: RelFileLocatorBackend) -> PgResult<()> {
    reln(key, |r| -> PgResult<()> {
        debug_assert!(r.pincount == 0, "smgrdestroy: pincount must be zero");
        for forknum in md::fork_iter() {
            match r.which {
                SmgrKind::Md => md::mdclose(&mut r.md, forknum)?,
            }
        }
        Ok(())
    })?;
    with_cache(|c| {
        if c.relns.remove(&key).is_none() {
            return Err(Box::new(PgError::error("SMgrRelation hashtable corrupted")));
        }
        c.unpinned.set(c.unpinned.get() - 1);
        Ok(())
    })
}

pub fn smgrdestroyall() -> PgResult<()> {
    // dlist_is_empty(&unpinned_relns): the steady state allocates nothing.
    if with_cache(|c| c.unpinned.get()) == 0 {
        return Ok(());
    }
    let keys = with_cache(|c| {
        let mut keys: PgVec<'static, RelFileLocatorBackend> = PgVec::new_in(c.cx.mcx());
        if keys.try_reserve(c.unpinned.get()).is_err() {
            return Err(oom("smgrdestroyall scratch"));
        }
        for (k, r) in c.relns.iter() {
            if r.pincount == 0 {
                keys.push(*k);
            }
        }
        Ok(keys)
    })?;
    for key in keys.iter() {
        smgrdestroy(*key)?;
    }
    Ok(())
}

pub fn smgrreleaseall() {
    let keys = with_cache(|c| {
        let mut keys: PgVec<'static, RelFileLocatorBackend> = PgVec::new_in(c.cx.mcx());
        if keys.try_reserve(c.relns.len()).is_err() {
            return keys;
        }
        for k in c.relns.keys() {
            keys.push(*k);
        }
        keys
    });
    for key in keys.iter() {
        // C's smgrreleaseall is void; md close failures are FATAL/LOG there.
        let _ = smgrrelease(*key);
    }
}

pub fn smgrreleaserellocator(key: RelFileLocatorBackend) -> PgResult<()> {
    if with_cache(|c| c.relns.contains_key(&key)) {
        smgrrelease(key)?;
    }
    Ok(())
}

pub fn ProcessBarrierSmgrRelease() -> PgResult<bool> {
    smgrreleaseall();
    Ok(true)
}

pub fn AtEOXact_SMgr() -> PgResult<()> {
    smgrdestroyall()
}

pub fn smgrsettargblock(key: RelFileLocatorBackend, targblock: BlockNumber) {
    with_reln(key, |r| r.smgr_targblock = targblock);
}

pub fn smgrgettargblock(key: RelFileLocatorBackend) -> BlockNumber {
    with_reln(key, |r| r.smgr_targblock).unwrap_or(InvalidBlockNumber)
}

pub fn smgrexists(key: RelFileLocatorBackend, forknum: ForkNumber) -> PgResult<bool> {
    reln(key, |r| match r.which {
        SmgrKind::Md => md::mdexists(key, &mut r.md, forknum),
    })
}

pub fn smgrcreate(key: RelFileLocatorBackend, forknum: ForkNumber, is_redo: bool) -> PgResult<()> {
    reln(key, |r| match r.which {
        SmgrKind::Md => md::mdcreate(key, &mut r.md, forknum, is_redo),
    })
}

pub fn smgrread(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: &mut [u8],
) -> PgResult<()> {
    let mut buffers: [&mut [u8]; 1] = [buffer];
    smgrreadv(key, forknum, blocknum, &mut buffers)
}

pub fn smgrwrite(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: &[u8],
    skip_fsync: bool,
) -> PgResult<()> {
    smgrwritev(key, forknum, blocknum, &[buffer], skip_fsync)
}

pub fn smgrreadv(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: &mut [&mut [u8]],
) -> PgResult<()> {
    reln(key, |r| match r.which {
        SmgrKind::Md => md::mdreadv(key, &mut r.md, forknum, blocknum, buffers),
    })
}

pub fn smgrwritev(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: &[&[u8]],
    skip_fsync: bool,
) -> PgResult<()> {
    reln(key, |r| match r.which {
        SmgrKind::Md => md::mdwritev(key, &mut r.md, forknum, blocknum, buffers, skip_fsync),
    })
}

pub fn smgrextend(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: &[u8],
    skip_fsync: bool,
) -> PgResult<()> {
    reln(key, |r| {
        match r.which {
            SmgrKind::Md => md::mdextend(key, &mut r.md, forknum, blocknum, buffer, skip_fsync)?,
        }
        update_cached_after_extend(r, forknum, blocknum, 1);
        Ok(())
    })
}

pub fn smgrzeroextend(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: i32,
    skip_fsync: bool,
) -> PgResult<()> {
    reln(key, |r| {
        match r.which {
            SmgrKind::Md => {
                md::mdzeroextend(key, &mut r.md, forknum, blocknum, nblocks, skip_fsync)?
            }
        }
        update_cached_after_extend(r, forknum, blocknum, nblocks as BlockNumber);
        Ok(())
    })
}

// Post-extend: advance the cached size if it was at blocknum, else invalidate.
fn update_cached_after_extend(
    r: &mut SMgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    added: BlockNumber,
) {
    let slot = &mut r.smgr_cached_nblocks[forknum as usize];
    if *slot == blocknum {
        *slot = blocknum + added;
    } else {
        *slot = InvalidBlockNumber;
    }
}

pub fn smgrprefetch(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: i32,
) -> PgResult<bool> {
    reln(key, |r| match r.which {
        SmgrKind::Md => md::mdprefetch(key, &mut r.md, forknum, blocknum, nblocks),
    })
}

pub fn smgrmaxcombine(
    key: RelFileLocatorBackend,
    _forknum: ForkNumber,
    blocknum: BlockNumber,
) -> u32 {
    match with_reln(key, |r| r.which).unwrap_or(SmgrKind::Md) {
        SmgrKind::Md => md::mdmaxcombine(blocknum),
    }
}

pub fn smgrwriteback(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: BlockNumber,
) -> PgResult<()> {
    reln(key, |r| match r.which {
        SmgrKind::Md => md::mdwriteback(key, &mut r.md, forknum, blocknum, nblocks),
    })
}

pub fn smgrfd(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
) -> PgResult<(i32, u32)> {
    reln(key, |r| match r.which {
        SmgrKind::Md => md::mdfd(key, &mut r.md, forknum, blocknum),
    })
}

pub fn smgrnblocks(key: RelFileLocatorBackend, forknum: ForkNumber) -> PgResult<BlockNumber> {
    reln(key, |r| {
        let cached = r.smgr_cached_nblocks[forknum as usize];
        // Believed only in recovery: no shared inval for fork-size changes.
        if xlogutils::in_recovery() && cached != InvalidBlockNumber {
            return Ok(cached);
        }
        let result = match r.which {
            SmgrKind::Md => md::mdnblocks(key, &mut r.md, forknum)?,
        };
        r.smgr_cached_nblocks[forknum as usize] = result;
        Ok(result)
    })
}

pub fn smgrnblocks_cached(key: RelFileLocatorBackend, forknum: ForkNumber) -> BlockNumber {
    if xlogutils::in_recovery() {
        if let Some(cached) = with_reln(key, |r| r.smgr_cached_nblocks[forknum as usize]) {
            return cached;
        }
    }
    InvalidBlockNumber
}

// Raw field read: fsm/vm/ExtendBufferedRelTo trust it outside recovery.
pub fn smgr_cached_nblocks_raw(key: RelFileLocatorBackend, forknum: ForkNumber) -> BlockNumber {
    with_reln(key, |r| r.smgr_cached_nblocks[forknum as usize]).unwrap_or(InvalidBlockNumber)
}

pub fn smgr_set_cached_nblocks(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    value: BlockNumber,
) -> PgResult<()> {
    opened(key, |r| r.smgr_cached_nblocks[forknum as usize] = value)
}

pub fn smgrtruncate(
    key: RelFileLocatorBackend,
    forknum: &[ForkNumber],
    old_nblocks: &[BlockNumber],
    nblocks: &[BlockNumber],
) -> PgResult<()> {
    debug_assert_eq!(forknum.len(), old_nblocks.len());
    debug_assert_eq!(forknum.len(), nblocks.len());

    bufmgr_seams::drop_relation_buffers::call(key, forknum, nblocks)?;
    inval::invalidate::CacheInvalidateSmgr(key)?;

    for i in 0..forknum.len() {
        reln(key, |r| -> PgResult<()> {
            // Invalid while truncating so an error leaves the cache unbelieved.
            r.smgr_cached_nblocks[forknum[i] as usize] = InvalidBlockNumber;
            match r.which {
                SmgrKind::Md => md::mdtruncate(key, &mut r.md, forknum[i], old_nblocks[i], nblocks[i])?,
            }
            // nblocks > old_nblocks happens on replica restart (md no-ops).
            r.smgr_cached_nblocks[forknum[i] as usize] = if nblocks[i] > old_nblocks[i] {
                old_nblocks[i]
            } else {
                nblocks[i]
            };
            Ok(())
        })?;
    }
    Ok(())
}

pub fn smgrregistersync(key: RelFileLocatorBackend, forknum: ForkNumber) -> PgResult<()> {
    reln(key, |r| match r.which {
        SmgrKind::Md => md::mdregistersync(key, &mut r.md, forknum),
    })
}

pub fn smgrimmedsync(key: RelFileLocatorBackend, forknum: ForkNumber) -> PgResult<()> {
    reln(key, |r| match r.which {
        SmgrKind::Md => md::mdimmedsync(key, &mut r.md, forknum),
    })
}

pub fn smgrdosyncall(rels: &[RelFileLocatorBackend]) -> PgResult<()> {
    if rels.is_empty() {
        return Ok(());
    }
    bufmgr_seams::flush_relations_all_buffers::call(rels)?;

    for &rel in rels {
        for forknum in md::fork_iter() {
            if smgrexists(rel, forknum)? {
                smgrimmedsync(rel, forknum)?;
            }
        }
    }
    Ok(())
}

pub fn smgrdounlinkall(rels: &[RelFileLocatorBackend], is_redo: bool) -> PgResult<()> {
    if rels.is_empty() {
        return Ok(());
    }
    bufmgr_seams::drop_relations_all_buffers::call(rels)?;

    for &rel in rels {
        reln(rel, |r| -> PgResult<()> {
            for forknum in md::fork_iter() {
                match r.which {
                    SmgrKind::Md => md::mdclose(&mut r.md, forknum)?,
                }
            }
            Ok(())
        })?;
    }

    // Sinval BEFORE unlinking, as a backstop if unlink fails partway.
    for &rel in rels {
        inval::invalidate::CacheInvalidateSmgr(rel)?;
    }

    // Unlink failure is a WARNING, not ERROR: the xact outcome is decided.
    for &rel in rels {
        for forknum in md::fork_iter() {
            match with_reln(rel, |r| r.which).unwrap_or(SmgrKind::Md) {
                SmgrKind::Md => md::mdunlink(rel, forknum, is_redo)?,
            }
        }
    }
    Ok(())
}

// DropRelationFiles: md.c source, homed here — pure smgr orchestration.
pub fn drop_relation_files(delrels: &[RelFileLocator], is_redo: bool) -> PgResult<()> {
    let mut srels: PgVec<'static, RelFileLocatorBackend> = PgVec::new_in(scratch_mcx());
    if srels.try_reserve(delrels.len()).is_err() {
        return Err(oom("DropRelationFiles srels"));
    }

    for &delrel in delrels {
        smgropen(delrel, INVALID_PROC_NUMBER)?;
        if is_redo {
            for fork in md::fork_iter() {
                xlogutils::XLogDropRelation(delrel, fork)?;
            }
        }
        srels.push(RelFileLocatorBackend { locator: delrel, backend: INVALID_PROC_NUMBER });
    }

    smgrdounlinkall(&srels, is_redo)?;

    for &srel in srels.iter() {
        smgrclose(srel)?;
    }
    Ok(())
}

pub fn ForgetDatabaseSyncRequests(dbid: ::types_core::Oid) -> PgResult<()> {
    md::ForgetDatabaseSyncRequests(dbid)
}

// mdsyncfiletag (md.c): homed here — it resolves through the handle cache.
pub fn mdsyncfiletag(ftag: FileTag) -> PgResult<FileTagOpResult> {
    let key = RelFileLocatorBackend { locator: ftag.rlocator, backend: INVALID_PROC_NUMBER };
    let forknum =
        ForkNumber::from_i32(ftag.forknum as i32).expect("FileTag.forknum is a ForkNumber");
    let fk = forknum as usize;

    let (file, need_to_close, path) = opened(key, |r| -> PgResult<(File, bool, String)> {
        if (ftag.segno as i64) < r.md.md_num_open_segs[fk] as i64 {
            let file = r.md.md_seg_fds[fk][ftag.segno as usize].mdfd_vfd;
            Ok((file, false, fd::FilePathName(file)))
        } else {
            let p = md::mdsegpath(key, forknum, ftag.segno as BlockNumber);
            let file = fd::PathNameOpenFile(&p, md::mdopenflags())?;
            Ok((file, true, p))
        }
    })??;

    if file.0 < 0 {
        return Ok(FileTagOpResult { result: -1, path, errno: md::last_errno() });
    }

    let result = if md::file_sync_failed(file, md::WAIT_EVENT_DATA_FILE_SYNC)? { -1 } else { 0 };
    let save_errno = md::last_errno();

    if need_to_close {
        fd::FileClose(file)?;
    }

    md::set_errno(save_errno);
    Ok(FileTagOpResult { result, path, errno: save_errno })
}

pub fn mdunlinkfiletag(ftag: FileTag) -> PgResult<FileTagOpResult> {
    md::mdunlinkfiletag(ftag)
}

pub fn mdfiletagmatches(ftag: FileTag, candidate: FileTag) -> bool {
    md::mdfiletagmatches(ftag, candidate)
}

pub fn init_seams() {
    smgr_seams::smgr_release_rel_locator::set(smgrreleaserellocator);
    smgr_seams::smgr_create::set(|rlocator, forknum, is_redo| {
        // smgrcreate(smgropen(rlocator), forknum, isRedo); open is idempotent.
        smgropen(rlocator.locator, rlocator.backend)?;
        smgrcreate(rlocator, forknum, is_redo)
    });
    smgr_seams::smgr_nblocks::set(|rlocator, forknum| {
        // smgrnblocks(smgropen(rlocator), forknum): C resolves the handle
        // once; one cache probe serves both the open and the nblocks body.
        opened(rlocator, |r| -> PgResult<BlockNumber> {
            let cached = r.smgr_cached_nblocks[forknum as usize];
            // Believed only in recovery: no shared inval for fork-size changes.
            if xlogutils::in_recovery() && cached != InvalidBlockNumber {
                return Ok(cached);
            }
            let result = match r.which {
                SmgrKind::Md => md::mdnblocks(rlocator, &mut r.md, forknum)?,
            };
            r.smgr_cached_nblocks[forknum as usize] = result;
            Ok(result)
        })?
    });
    smgr_seams::smgr_read::set(|rlocator, forknum, blocknum, buffer| {
        // smgrread(smgropen(rlocator), ...): one probe, as above.
        opened(rlocator, |r| {
            let mut buffers: [&mut [u8]; 1] = [buffer];
            match r.which {
                SmgrKind::Md => md::mdreadv(rlocator, &mut r.md, forknum, blocknum, &mut buffers),
            }
        })?
    });
    smgr_seams::smgr_destroy_all::set(smgrdestroyall);
    smgr_seams::at_eoxact_smgr::set(|| {
        // C's AtEOXact_SMgr is void; md close failures are not ERROR in C.
        let _ = AtEOXact_SMgr();
    });
    smgr_seams::process_barrier_smgr_release::set(ProcessBarrierSmgrRelease);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(rel: u32) -> RelFileLocatorBackend {
        RelFileLocatorBackend {
            locator: RelFileLocator { spcOid: 1, dbOid: 2, relNumber: rel },
            backend: INVALID_PROC_NUMBER,
        }
    }

    fn contains(k: RelFileLocatorBackend) -> bool {
        with_cache(|c| c.relns.contains_key(&k))
    }

    #[test]
    fn open_is_idempotent_and_destroyall_reclaims_unpinned() {
        let k = key(31001);
        smgropen(k.locator, k.backend).unwrap();
        smgropen(k.locator, k.backend).unwrap();
        assert!(contains(k));
        smgrdestroyall().unwrap();
        assert!(!contains(k), "unpinned entry must be destroyed at EOXact");
    }

    #[test]
    fn relcache_pin_survives_destroyall_and_is_idempotent() {
        let k = key(31002);
        smgropen(k.locator, k.backend).unwrap();
        smgrpin_relcache(k);
        smgrpin_relcache(k);
        assert_eq!(with_reln(k, |r| r.pincount), Some(1));
        smgrdestroyall().unwrap();
        assert!(contains(k), "pinned entry must survive smgrdestroyall");
        smgrunpin_relcache(k);
        assert_eq!(with_reln(k, |r| r.pincount), Some(0));
        smgrdestroyall().unwrap();
        assert!(!contains(k));
    }

    #[test]
    fn targblock_defaults_invalid_and_roundtrips() {
        let k = key(31003);
        assert_eq!(smgrgettargblock(k), InvalidBlockNumber);
        smgropen(k.locator, k.backend).unwrap();
        assert_eq!(smgrgettargblock(k), InvalidBlockNumber);
        smgrsettargblock(k, 42);
        assert_eq!(smgrgettargblock(k), 42);
        smgrrelease(k).unwrap();
        assert_eq!(smgrgettargblock(k), InvalidBlockNumber);
        smgrdestroy(k).unwrap();
    }

    #[test]
    fn cached_nblocks_raw_field_semantics() {
        let k = key(31004);
        assert_eq!(smgr_cached_nblocks_raw(k, ForkNumber::MAIN_FORKNUM), InvalidBlockNumber);
        smgr_set_cached_nblocks(k, ForkNumber::MAIN_FORKNUM, 7).unwrap();
        assert_eq!(smgr_cached_nblocks_raw(k, ForkNumber::MAIN_FORKNUM), 7);
        assert_eq!(smgrnblocks_cached(k, ForkNumber::MAIN_FORKNUM), InvalidBlockNumber);
        smgrdestroy(k).unwrap();
    }

    #[test]
    fn extend_cache_update_advances_or_invalidates() {
        let k = key(31005);
        smgropen(k.locator, k.backend).unwrap();
        with_reln(k, |r| {
            r.smgr_cached_nblocks[0] = 10;
            update_cached_after_extend(r, ForkNumber::MAIN_FORKNUM, 10, 1);
            assert_eq!(r.smgr_cached_nblocks[0], 11);
            update_cached_after_extend(r, ForkNumber::MAIN_FORKNUM, 20, 1);
            assert_eq!(r.smgr_cached_nblocks[0], InvalidBlockNumber);
        });
        smgrdestroy(k).unwrap();
    }

    #[test]
    fn maxcombine_geometry_matches_md() {
        let k = key(31006);
        assert_eq!(
            smgrmaxcombine(k, ForkNumber::MAIN_FORKNUM, 0),
            ::types_storage::smgr::RELSEG_SIZE
        );
        assert_eq!(
            smgrmaxcombine(k, ForkNumber::MAIN_FORKNUM, ::types_storage::smgr::RELSEG_SIZE - 1),
            1
        );
    }
}
