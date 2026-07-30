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
use ::types_rel::RelationData;
use ::types_storage::file::File;
use ::types_storage::smgr::{MdRelnState, SmgrHandle, SMGR_NFORKS};
use ::types_storage::sync::{FileTag, FileTagOpResult};
use ::types_storage::{RelFileLocator, RelFileLocatorBackend, WriteChunk};

// smgrsw[]: closed set; a new manager is a new variant (rule 4).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SmgrKind {
    Md,
}

pub struct SMgrRelation {
    pub smgr_rlocator: RelFileLocatorBackend,
    pub smgr_targblock: BlockNumber,
    pub smgr_cached_nblocks: [BlockNumber; SMGR_NFORKS],
    which: SmgrKind,
    pincount: i32,
    // C's `rd_smgr != NULL`: keeps the relcache's single pin idempotent.
    relcache_pinned: bool,
    md: MdRelnState,
}

const _: () = assert!(core::mem::size_of::<SMgrRelation>() <= 168);

impl SMgrRelation {
    fn new(smgr_rlocator: RelFileLocatorBackend) -> Self {
        SMgrRelation {
            smgr_rlocator,
            smgr_targblock: InvalidBlockNumber,
            smgr_cached_nblocks: [InvalidBlockNumber; SMGR_NFORKS],
            which: SmgrKind::Md,
            pincount: 0,
            relcache_pinned: false,
            md: MdRelnState::default(),
        }
    }
}

// C's stable SMgrRelation pointer, as a slab slot: slots never move, so an
// SmgrHandle (idx+gen) survives map rehash; destroy bumps gen so a stale
// cached handle fails loudly instead of aliasing a recycled slot.
struct Slot {
    gen: u32,
    reln: Option<SMgrRelation>,
}

struct SmgrCache {
    cx: &'static MemoryContext,
    relns: PgHashMap<'static, RelFileLocatorBackend, u32>,
    // std Vec justified: backend-lifetime owner structure already holding md's
    // droppy fd vectors (MdRelnState precedent), outside context accounting.
    slab: Vec<Slot>,
    free: Vec<u32>,
    // |unpinned_relns|: AtEOXact_SMgr skips the walk when all are pinned.
    unpinned: Cell<usize>,
}

impl SmgrCache {
    fn slot(&mut self, key: RelFileLocatorBackend) -> Option<&mut SMgrRelation> {
        let idx = *self.relns.get(&key)?;
        self.slab[idx as usize].reln.as_mut()
    }

    fn handle_for(&self, idx: u32) -> SmgrHandle {
        SmgrHandle {
            idx,
            gen: core::num::NonZeroU32::new(self.slab[idx as usize].gen)
                .expect("smgr slot gen is never zero"),
        }
    }

}

thread_local! {
    static CACHE: RefCell<Option<SmgrCache>> = const { RefCell::new(None) };
}

#[track_caller]
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
                ::mcx::session_root("smgr relation table");
            // LIFO: empty the droppy TLS cache before its context is freed.
            ::mcx::register_session_cleanup(Box::new(|| {
                CACHE.with(|c| drop(c.borrow_mut().take()));
            }));
            let mut relns = PgHashMap::new_in(cx.mcx());
            let _ = relns.try_reserve(400);
            SmgrCache { cx, relns, slab: Vec::new(), free: Vec::new(), unpinned: Cell::new(0) }
        });
        f(cache)
    })
}

fn scratch_mcx() -> Mcx<'static> {
    with_cache(|c| c.cx.mcx())
}

fn with_reln<R>(key: RelFileLocatorBackend, f: impl FnOnce(&mut SMgrRelation) -> R) -> Option<R> {
    with_cache(|c| c.slot(key).map(f))
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
fn open_entry(c: &mut SmgrCache, key: RelFileLocatorBackend) -> PgResult<u32> {
    if c.relns.len() == c.relns.capacity() && c.relns.try_reserve(1).is_err() {
        return Err(oom("SMgrRelation hashtable"));
    }
    let mut r = SMgrRelation::new(key);
    match r.which {
        SmgrKind::Md => md::mdopen(&mut r.md),
    }
    let idx = match c.free.pop() {
        Some(idx) => {
            let slot = &mut c.slab[idx as usize];
            debug_assert!(slot.reln.is_none());
            slot.reln = Some(r);
            idx
        }
        None => {
            let idx = u32::try_from(c.slab.len()).expect("smgr slab index fits u32");
            c.slab.push(Slot { gen: 1, reln: Some(r) });
            idx
        }
    };
    c.relns.insert(key, idx);
    c.unpinned.set(c.unpinned.get() + 1);
    Ok(idx)
}

fn opened_idx(c: &mut SmgrCache, key: RelFileLocatorBackend) -> PgResult<u32> {
    debug_assert!(key.locator.relNumber != 0, "smgropen: invalid RelFileNumber");
    // Warm hit = ONE probe (C smgropen's HASH_ENTER-found), no capacity
    // bookkeeping.
    if let Some(&idx) = c.relns.get(&key) {
        return Ok(idx);
    }
    open_entry(c, key)
}

fn opened<R>(
    key: RelFileLocatorBackend,
    f: impl FnOnce(&mut SMgrRelation) -> R,
) -> PgResult<R> {
    with_cache(|c| {
        let idx = opened_idx(c, key)?;
        Ok(f(c.slab[idx as usize].reln.as_mut().expect("open smgr slot is live")))
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

pub fn smgropen_handle(rlocator: RelFileLocator, backend: ProcNumber) -> PgResult<SmgrHandle> {
    let key = RelFileLocatorBackend { locator: rlocator, backend };
    with_cache(|c| {
        let idx = opened_idx(c, key)?;
        Ok(c.handle_for(idx))
    })
}

fn with_reln_at(
    c: &mut SmgrCache,
    key: RelFileLocatorBackend,
    f: impl FnOnce(&mut SMgrRelation, &Cell<usize>),
) {
    if let Some(&idx) = c.relns.get(&key) {
        if let Some(r) = c.slab[idx as usize].reln.as_mut() {
            f(r, &c.unpinned);
        }
    }
}

pub fn smgrpin(key: RelFileLocatorBackend) {
    with_cache(|c| {
        with_reln_at(c, key, |r, unpinned| {
            let old = r.pincount;
            r.pincount += 1;
            note_pin_transition(unpinned, old, r.pincount);
        })
    });
}

pub fn smgrunpin(key: RelFileLocatorBackend) {
    with_cache(|c| {
        with_reln_at(c, key, |r, unpinned| {
            debug_assert!(r.pincount > 0, "smgrunpin: pincount must be positive");
            let old = r.pincount;
            r.pincount -= 1;
            note_pin_transition(unpinned, old, r.pincount);
        })
    });
}

pub fn smgrpin_relcache(key: RelFileLocatorBackend) {
    with_cache(|c| {
        with_reln_at(c, key, |r, unpinned| {
            if !r.relcache_pinned {
                r.relcache_pinned = true;
                let old = r.pincount;
                r.pincount += 1;
                note_pin_transition(unpinned, old, r.pincount);
            }
        })
    });
}

pub fn smgrunpin_relcache(key: RelFileLocatorBackend) {
    with_cache(|c| {
        with_reln_at(c, key, |r, unpinned| {
            if r.relcache_pinned {
                r.relcache_pinned = false;
                let old = r.pincount;
                r.pincount -= 1;
                note_pin_transition(unpinned, old, r.pincount);
            }
        })
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
        let Some(idx) = c.relns.remove(&key) else {
            return Err(Box::new(PgError::error("SMgrRelation hashtable corrupted")));
        };
        let slot = &mut c.slab[idx as usize];
        slot.reln = None;
        // gen 0 is the Option<SmgrHandle> niche; skip it on wrap.
        slot.gen = slot.gen.wrapping_add(1);
        if slot.gen == 0 {
            slot.gen = 1;
        }
        c.free.push(idx);
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
        for (k, &idx) in c.relns.iter() {
            if c.slab[idx as usize].reln.as_ref().is_some_and(|r| r.pincount == 0) {
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
    smgrwritev(key, forknum, blocknum, &[WriteChunk::from_slice(buffer)], skip_fsync)
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
    buffers: &[WriteChunk<'_>],
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

    let io_start = pgstat::io::pgstat_prepare_io_time(md::track_io_timing());

    let result = if md::file_sync_failed(file, md::WAIT_EVENT_DATA_FILE_SYNC)? { -1 } else { 0 };
    let save_errno = md::last_errno();

    if need_to_close {
        fd::FileClose(file)?;
    }

    pgstat::io::pgstat_count_io_op_time(
        pgstat::io::IOObject::Relation,
        pgstat::io::IOContext::IOCONTEXT_NORMAL,
        pgstat::io::IOOp::Fsync,
        io_start,
        1,
        0,
    );

    md::set_errno(save_errno);
    Ok(FileTagOpResult { result, path, errno: save_errno })
}

pub fn mdunlinkfiletag(ftag: FileTag) -> PgResult<FileTagOpResult> {
    md::mdunlinkfiletag(ftag)
}

pub fn mdfiletagmatches(ftag: FileTag, candidate: FileTag) -> bool {
    md::mdfiletagmatches(ftag, candidate)
}

fn with_handle<R>(h: SmgrHandle, f: impl FnOnce(&mut SMgrRelation, &Cell<usize>) -> R) -> R {
    with_cache(|c| {
        let slot = &mut c.slab[h.idx as usize];
        assert!(
            slot.gen == h.gen.get() && slot.reln.is_some(),
            "stale SmgrHandle: smgr entry destroyed under a cached rd_smgr (pin contract violated)"
        );
        f(slot.reln.as_mut().unwrap(), &c.unpinned)
    })
}

pub fn smgrpin_h(h: SmgrHandle) {
    with_handle(h, |r, unpinned| {
        let old = r.pincount;
        r.pincount += 1;
        note_pin_transition(unpinned, old, r.pincount);
    })
}

pub fn smgrunpin_h(h: SmgrHandle) {
    with_handle(h, |r, unpinned| {
        debug_assert!(r.pincount > 0, "smgrunpin: pincount must be positive");
        let old = r.pincount;
        r.pincount -= 1;
        note_pin_transition(unpinned, old, r.pincount);
    })
}

pub fn smgrnblocks_h(h: SmgrHandle, forknum: ForkNumber) -> PgResult<BlockNumber> {
    with_handle(h, |r, _| {
        let cached = r.smgr_cached_nblocks[forknum as usize];
        // Believed only in recovery: no shared inval for fork-size changes.
        if xlogutils::in_recovery() && cached != InvalidBlockNumber {
            return Ok(cached);
        }
        let key = r.smgr_rlocator;
        let result = match r.which {
            SmgrKind::Md => md::mdnblocks(key, &mut r.md, forknum)?,
        };
        r.smgr_cached_nblocks[forknum as usize] = result;
        Ok(result)
    })
}

pub fn smgrgettargblock_h(h: SmgrHandle) -> BlockNumber {
    with_handle(h, |r, _| r.smgr_targblock)
}

pub fn smgrsettargblock_h(h: SmgrHandle, targblock: BlockNumber) {
    with_handle(h, |r, _| r.smgr_targblock = targblock);
}

pub fn smgrclose_h(h: SmgrHandle) -> PgResult<()> {
    let key = with_handle(h, |r, _| r.smgr_rlocator);
    smgrrelease(key)
}

// rel.h RelationGetSmgr: warm hit is a plain Cell load, zero probes (the
// relcache pin keeps the slot alive, so the cached handle cannot go stale).
pub fn RelationGetSmgr(rel: &RelationData<'_>) -> PgResult<SmgrHandle> {
    if let Some(h) = rel.rd_smgr.get() {
        return Ok(h);
    }
    rel_open_smgr(rel)
}

// One plain pin per cached handle (C pins per relcache-entry pointer; a
// rebuilt entry coexisting with held old entries must each hold their own).
#[cold]
#[inline(never)]
fn rel_open_smgr(rel: &RelationData<'_>) -> PgResult<SmgrHandle> {
    let h = smgropen_handle(rel_file_locator(rel), rel.rd_backend)?;
    smgrpin_h(h);
    rel.rd_smgr.set(Some(h));
    Ok(h)
}

/// rd_locator read; the compute arm backfills entries built outside relcache
/// (tests, pre-InitPhysicalAddr builds) with RelationInitPhysicalAddr's
/// steady-state rules — C's invariant is "valid before any smgr access".
/// Twin of bufmgr::rel_locator_backend's compute arm (seam boundary keeps
/// the crates apart).
pub fn rel_file_locator(rel: &RelationData<'_>) -> RelFileLocator {
    let locator = rel.rd_locator.get();
    if locator.relNumber != 0 {
        return locator;
    }
    compute_rel_locator(rel)
}

const DEFAULTTABLESPACE_OID: ::types_core::Oid = 1663;
const GLOBALTABLESPACE_OID: ::types_core::Oid = 1664;

#[cold]
#[inline(never)]
fn compute_rel_locator(rel: &RelationData<'_>) -> RelFileLocator {
    let form = &rel.rd_rel;
    let rel_number = if form.relfilenode == 0 {
        let n = relmapper_seams::relation_map_oid_to_filenumber::call(rel.rd_id, form.relisshared);
        // C elog(ERROR)s on a missing mapping; can't-happen once maps load.
        assert!(
            n != 0,
            "could not find relation mapping for relation \"{}\", OID {}",
            String::from_utf8_lossy(form.relname.name_str()),
            rel.rd_id
        );
        n
    } else {
        form.relfilenode
    };
    let spc = if form.reltablespace != 0 {
        form.reltablespace
    } else {
        DEFAULTTABLESPACE_OID
    };
    let db = if spc == GLOBALTABLESPACE_OID {
        0
    } else {
        init_small::globals::MyDatabaseId()
    };
    let locator = RelFileLocator { spcOid: spc, dbOid: db, relNumber: rel_number };
    rel.rd_locator.set(locator);
    locator
}

// rel.h RelationCloseSmgr.
pub fn RelationCloseSmgr(rel: &RelationData<'_>) -> PgResult<()> {
    let Some(h) = rel.rd_smgr.get() else {
        return Ok(());
    };
    smgrunpin_h(h);
    rel.rd_smgr.set(None);
    smgrclose_h(h)
}

pub fn init_seams() {
    smgr_seams::smgr_release_rel_locator::set(smgrreleaserellocator);
    smgr_seams::smgr_exists::set(|rlocator, forknum| {
        // smgrexists(smgropen(rlocator), forknum): one probe, as smgr_nblocks.
        opened(rlocator, |r| match r.which {
            SmgrKind::Md => md::mdexists(rlocator, &mut r.md, forknum),
        })?
    });
    smgr_seams::smgr_cached_nblocks::set(smgr_cached_nblocks_raw);
    smgr_seams::smgr_nblocks_cached::set(smgrnblocks_cached);
    smgr_seams::smgr_set_cached_nblocks::set(smgr_set_cached_nblocks);
    smgr_seams::smgr_nblocks_cache_purge_db::set(md::nblocks_cache::purge_db);
    smgr_seams::smgr_nblocks_cache_clear::set(md::nblocks_cache::clear);
    smgr_seams::smgr_nblocks_cache_poison::set(|rlocator, forknum| {
        // Temp relations never enter the process-global cache; nothing to
        // poison for them.
        if rlocator.backend == INVALID_PROC_NUMBER {
            md::nblocks_cache::poison(rlocator.locator, forknum);
        }
    });
    smgr_seams::smgr_create::set(|rlocator, forknum, is_redo| {
        // smgrcreate(smgropen(rlocator), forknum, isRedo); open is idempotent.
        smgropen(rlocator.locator, rlocator.backend)?;
        smgrcreate(rlocator, forknum, is_redo)
    });
    smgr_seams::rel_smgr_nblocks::set(|rel, forknum| smgrnblocks_h(RelationGetSmgr(rel)?, forknum));
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
    smgr_seams::smgr_prefetch::set(|rlocator, forknum, blocknum, nblocks| {
        opened(rlocator, |r| match r.which {
            SmgrKind::Md => md::mdprefetch(rlocator, &mut r.md, forknum, blocknum, nblocks),
        })?
    });
    smgr_seams::smgr_start_buffer_read::set(|rlocator, forknum, blocknum, buffer| {
        opened(rlocator, |r| match r.which {
            SmgrKind::Md => md::mdstartbufread(rlocator, &mut r.md, forknum, blocknum, buffer),
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
    smgr_seams::smgr_readv::set(|rlocator, forknum, blocknum, buffers| {
        opened(rlocator, |r| match r.which {
            SmgrKind::Md => md::mdreadv(rlocator, &mut r.md, forknum, blocknum, buffers),
        })?
    });
    smgr_seams::smgr_startreadv::set(|rlocator, forknum, blocknum, pages| {
        // smgrstartreadv: interrupts held so the resolved fd cannot be closed
        // before pgaio_io_start_readv consumes it; an ereport unwinds past the
        // resume, exactly like C's longjmp (error recovery resets the count).
        init_small::globals::HoldInterrupts();
        let result = opened(rlocator, |r| match r.which {
            SmgrKind::Md => md::mdstartreadv(rlocator, &mut r.md, forknum, blocknum, pages),
        })
        .and_then(|inner| inner);
        if result.is_ok() {
            init_small::globals::ResumeInterrupts();
        }
        result
    });
    smgr_seams::aio_smgr_reopen::set(|td, op, temp_procno, offset| {
        let key = RelFileLocatorBackend {
            locator: td.smgr.rlocator,
            backend: if td.smgr.is_temp { temp_procno } else { INVALID_PROC_NUMBER },
        };
        match op {
            types_storage::aio::PGAIO_OP_READV => opened(key, |r| match r.which {
                SmgrKind::Md => {
                    md::md_aio_reopen_fd(key, &mut r.md, td.smgr.forkNum, td.smgr.blockNum, offset)
                }
            })?,
            _ => panic!("unported arm reached from smgr.c smgr_aio_reopen: writev"),
        }
    });
    smgr_seams::aio_md_readv_complete::set(md::md_readv_complete);
    smgr_seams::aio_md_readv_report::set(md::md_readv_report);
    smgr_seams::smgr_write::set(|rlocator, forknum, blocknum, buffer, skip_fsync| {
        opened(rlocator, |r| match r.which {
            SmgrKind::Md => {
                md::mdwritev(rlocator, &mut r.md, forknum, blocknum, &[buffer], skip_fsync)
            }
        })?
    });
    smgr_seams::smgr_zeroextend::set(smgrzeroextend);
    smgr_seams::smgr_writeback::set(|rlocator, forknum, blocknum, nblocks| {
        opened(rlocator, |r| match r.which {
            SmgrKind::Md => md::mdwriteback(rlocator, &mut r.md, forknum, blocknum, nblocks),
        })?
    });
    smgr_seams::smgr_destroy_all::set(smgrdestroyall);
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
    fn handle_stable_across_rehash_and_release() {
        let k = key(31007);
        let h = smgropen_handle(k.locator, k.backend).unwrap();
        smgrpin_h(h);
        // Force map growth past the initial reservation; slab slots must not move.
        for i in 0..600u32 {
            smgropen(RelFileLocator { spcOid: 1, dbOid: 2, relNumber: 40000 + i }, k.backend)
                .unwrap();
        }
        assert_eq!(with_handle(h, |r, _| r.smgr_rlocator), k);
        smgrrelease(k).unwrap();
        assert_eq!(with_handle(h, |r, _| r.smgr_targblock), InvalidBlockNumber);
        smgrdestroyall().unwrap();
        assert!(contains(k), "pinned entry must survive smgrdestroyall");
        smgrunpin_h(h);
        smgrdestroyall().unwrap();
        assert!(!contains(k));
    }

    #[test]
    fn stale_handle_fails_loudly_and_slot_gen_advances() {
        let k = key(31008);
        let h = smgropen_handle(k.locator, k.backend).unwrap();
        smgrdestroy(k).unwrap();
        let r = std::panic::catch_unwind(|| with_handle(h, |_, _| ()));
        assert!(r.is_err(), "stale handle use must panic");
        let k2 = key(31009);
        let h2 = smgropen_handle(k2.locator, k2.backend).unwrap();
        if h2.idx == h.idx {
            assert_ne!(h2.gen, h.gen, "slot reuse must bump gen");
        }
        smgrdestroy(k2).unwrap();
    }

    fn test_rel(k: RelFileLocatorBackend) -> RelationData<'static> {
        use ::types_rel::{FormData_pg_class, LockInfoData, LockRelId};
        use ::types_tuple::{NameData, TupleDescData};
        use core::cell::Cell;
        use std::rc::Rc;
        let cx: &'static MemoryContext = ::mcx::session_root("test rel");
        let mut relname = NameData::default();
        relname.namestrcpy("t");
        let mut form = FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: 2,
            relfilenode: k.locator.relNumber,
            reltablespace: k.locator.spcOid,
            relpages: 0,
            reltuples: -1.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence: ::types_core::RELPERSISTENCE_PERMANENT,
            relkind: ::types_rel::RELKIND_RELATION,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: b'd',
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        };
        form.relfilenode = k.locator.relNumber;
        RelationData {
            rd_locator: Cell::new(k.locator),
            rd_smgr: Cell::new(None),
            rd_id: k.locator.relNumber,
            rd_backend: k.backend,
            rd_islocaltemp: false,
            rd_isvalid: Cell::new(true),
            rd_createSubid: Cell::new(0),
            rd_newRelfilelocatorSubid: Cell::new(0),
            rd_firstRelfilelocatorSubid: Cell::new(0),
            rd_droppedSubid: Cell::new(0),
            rd_lockInfo: LockInfoData {
                lockRelId: LockRelId { relId: k.locator.relNumber, dbId: k.locator.dbOid },
            },
            rd_rel: form,
            rd_att: Rc::new(TupleDescData {
                natts: 0,
                tdtypeid: 0,
                tdtypmod: -1,
                tdrefcount: 1,
                constr: None,
                compact_attrs: PgVec::new_in(cx.mcx()),
                attrs: PgVec::new_in(cx.mcx()),
            }),
            rd_index: None,
            rd_opcintype: PgVec::new_in(cx.mcx()),
            rd_opfamily: PgVec::new_in(cx.mcx()),
            rd_indoption: PgVec::new_in(cx.mcx()),
            rd_indcollation: PgVec::new_in(cx.mcx()),
            rd_options: None,
            pgstat_enabled: Cell::new(false),
            pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
            rd_amcache: Default::default(),
            rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
            rd_support: PgVec::new_in(cx.mcx()),
            rd_supportinfo: Default::default(),
            rd_opcoptions: Default::default(),
            rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
        }
    }

    #[test]
    fn relation_get_smgr_caches_and_pins() {
        let k = key(31010);
        let rel = test_rel(k);
        let h = RelationGetSmgr(&rel).unwrap();
        assert_eq!(rel.rd_smgr.get(), Some(h));
        let h2 = RelationGetSmgr(&rel).unwrap();
        assert_eq!(h, h2);
        smgrdestroyall().unwrap();
        assert!(contains(k), "rd_smgr pin must survive smgrdestroyall");
        RelationCloseSmgr(&rel).unwrap();
        assert_eq!(rel.rd_smgr.get(), None);
        RelationCloseSmgr(&rel).unwrap();
        smgrdestroyall().unwrap();
        assert!(!contains(k));
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
