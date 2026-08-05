// InvalidateBuffer / FindAndDropRelationBuffers / DropRelationsAllBuffers
// (bufmgr.c); temp-relation arms divert to localbuf.
use types_core::{BlockNumber, ForkNumber, InvalidBlockNumber, MAX_FORKNUM};
use types_error::PgResult;
use types_storage::buf::{BM_TAG_VALID, BUF_FLAG_MASK, BUF_USAGECOUNT_MASK};
use crate::pin::buffer_refcount;
use types_storage::{RelFileLocator, RelFileLocatorBackend};

use crate::buf_hdr::{
    cleared_buftag, BufferDesc, BufferDescriptorGetBuffer, GetBufferDescriptor, LockBufHdr,
    NBuffersInited, UnlockBufHdr,
};
use crate::buf_table::{BufMappingPartitionLock, BufTableDelete, BufTableHashCode, BufTableLookup};
use crate::freelist::StrategyFreeBuffer;
use lwlock::{LWLockAcquire, LWLockRelease, LW_EXCLUSIVE, LW_SHARED};

pub(crate) const RELS_BSEARCH_THRESHOLD: usize = 20;

/// DropRelationBuffers (bufmgr.c).
pub fn DropRelationBuffers(
    rlocator: RelFileLocatorBackend,
    forknum: &[ForkNumber],
    first_del_block: &[BlockNumber],
) -> PgResult<()> {
    if rlocator.backend != types_core::INVALID_PROC_NUMBER {
        if rlocator.backend == init_small::globals::MyProcNumber() {
            for (fork, first) in forknum.iter().zip(first_del_block) {
                crate::localbuf::DropRelationLocalBuffers(rlocator.locator, *fork, *first)?;
            }
        }
        return Ok(());
    }

    // Cached-size optimization: probe only the doomed block range when every
    // fork size is cached and the range is small (recovery guarantees the
    // cache; elsewhere a stale cache just forfeits the fast path).
    let mut n_fork_block = [InvalidBlockNumber; MAX_FORKNUM as usize + 1];
    let mut n_blocks_to_invalidate: u64 = 0;
    let mut cached = true;
    for (i, fork) in forknum.iter().enumerate() {
        let nblocks = smgr_seams::smgr_nblocks_cached::call(rlocator, *fork);
        if nblocks == InvalidBlockNumber {
            cached = false;
            break;
        }
        n_fork_block[i] = nblocks;
        n_blocks_to_invalidate += (nblocks - first_del_block[i]) as u64;
    }

    if cached && n_blocks_to_invalidate < buf_drop_full_scan_threshold() {
        for (i, fork) in forknum.iter().enumerate() {
            FindAndDropRelationBuffers(
                rlocator.locator,
                *fork,
                n_fork_block[i],
                first_del_block[i],
            )?;
        }
        return Ok(());
    }

    for i in 0..NBuffersInited() {
        let desc = GetBufferDescriptor(i);
        // Unlocked precheck, safe as in C (both tag halves change under the
        // mapping lock; a false miss here was concurrently invalidated).
        if !tag_matches(&desc.tag(), &rlocator.locator) {
            continue;
        }
        let buf_state = LockBufHdr(desc);
        let tag = desc.tag();
        let doomed = tag_matches(&tag, &rlocator.locator)
            && forknum
                .iter()
                .zip(first_del_block)
                .any(|(fork, first)| tag.forkNum == *fork && tag.blockNum >= *first);
        if doomed {
            InvalidateBuffer(desc, buf_state)?;
        } else {
            UnlockBufHdr(desc, buf_state);
        }
    }
    Ok(())
}

fn buf_drop_full_scan_threshold() -> u64 {
    (NBuffersInited() as u64) / 32
}

pub(crate) fn tag_matches(tag: &types_storage::buf::buftag, rlocator: &RelFileLocator) -> bool {
    tag.spcOid == rlocator.spcOid
        && tag.dbOid == rlocator.dbOid
        && tag.relNumber == rlocator.relNumber
}

// InvalidateBuffer: caller holds the buffer header lock; released on return.
fn InvalidateBuffer(desc: &BufferDesc, buf_state_in: u32) -> PgResult<()> {
    let old_tag = desc.tag();
    UnlockBufHdr(desc, buf_state_in);

    let old_hash = BufTableHashCode(&old_tag);
    let old_partition_lock = BufMappingPartitionLock(old_hash);

    loop {
        LWLockAcquire(old_partition_lock, LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
        let mut buf_state = LockBufHdr(desc);

        if desc.tag() != old_tag {
            UnlockBufHdr(desc, buf_state);
            LWLockRelease(old_partition_lock)?;
            return Ok(());
        }

        if buffer_refcount(buf_state) != 0 {
            UnlockBufHdr(desc, buf_state);
            LWLockRelease(old_partition_lock)?;
            let mut private = crate::privref::GetPrivateRefCount(BufferDescriptorGetBuffer(desc));
            if private > 0 {
                // Uncollected uring prefetch reads hold thread-owned pins
                // (LockBufferForCleanup precedent); wait them out before the
                // C safety check — C's fadvise prefetch never holds pins.
                crate::uring::drain_own();
                private = crate::privref::GetPrivateRefCount(BufferDescriptorGetBuffer(desc));
            }
            if private > 0 {
                let pins: Vec<String> = crate::privref::debug_all_private_pins()
                    .into_iter()
                    .map(|(b, rc)| {
                        let t = GetBufferDescriptor(b - 1).tag();
                        format!(
                            "buf={b} rc={rc} tag=({}/{}/{} fork={} blk={})",
                            t.spcOid, t.dbOid, t.relNumber, t.forkNum as i32, t.blockNum
                        )
                    })
                    .collect();
                panic!(
                    "buffer is pinned in InvalidateBuffer: buf={} tag=({}/{}/{} fork={} blk={}) shared_refcount={} private_refcount={}; all private pins: [{}]",
                    BufferDescriptorGetBuffer(desc),
                    old_tag.spcOid,
                    old_tag.dbOid,
                    old_tag.relNumber,
                    old_tag.forkNum as i32,
                    old_tag.blockNum,
                    buffer_refcount(buf_state),
                    private,
                    pins.join(", "),
                );
            }
            crate::read::WaitIO(desc)?;
            continue;
        }

        let old_flags = buf_state & BUF_FLAG_MASK;
        // SAFETY: header lock held; refcount is zero (checked above).
        unsafe { desc.set_tag(cleared_buftag()) };
        buf_state &= !(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
        UnlockBufHdr(desc, buf_state);

        if old_flags & BM_TAG_VALID != 0 {
            BufTableDelete(&old_tag, old_hash)?;
        }
        LWLockRelease(old_partition_lock)?;

        StrategyFreeBuffer(BufferDescriptorGetBuffer(desc) - 1);
        return Ok(());
    }
}

fn FindAndDropRelationBuffers(
    rlocator: RelFileLocator,
    fork_num: ForkNumber,
    n_fork_block: BlockNumber,
    first_del_block: BlockNumber,
) -> PgResult<()> {
    for cur_block in first_del_block..n_fork_block {
        let tag = types_storage::buf::buftag {
            spcOid: rlocator.spcOid,
            dbOid: rlocator.dbOid,
            relNumber: rlocator.relNumber,
            forkNum: fork_num,
            blockNum: cur_block,
        };
        let hash = BufTableHashCode(&tag);
        let partition_lock = BufMappingPartitionLock(hash);
        LWLockAcquire(partition_lock, LW_SHARED, init_small::globals::MyProcNumber())?;
        let buf_id = BufTableLookup(&tag, hash)?;
        LWLockRelease(partition_lock)?;
        if buf_id < 0 {
            continue;
        }
        let desc = GetBufferDescriptor(buf_id);
        let buf_state = LockBufHdr(desc);
        let dtag = desc.tag();
        if tag_matches(&dtag, &rlocator)
            && dtag.forkNum == fork_num
            && dtag.blockNum >= first_del_block
        {
            InvalidateBuffer(desc, buf_state)?;
        } else {
            UnlockBufHdr(desc, buf_state);
        }
    }
    Ok(())
}

/// DropDatabaseBuffers (bufmgr.c): whole-pool sweep; no relation-count
/// optimization is possible, as in C.
pub fn DropDatabaseBuffers(dbid: types_core::Oid) -> PgResult<()> {
    // The callers (drop/move database) are about to rmtree the database's
    // file tree — sizes change behind md's back, so its process-global size
    // cache must forget the whole database.
    smgr_seams::smgr_nblocks_cache_purge_db::call(dbid);
    for i in 0..NBuffersInited() {
        let desc = GetBufferDescriptor(i);
        // Unlocked precheck, safe as in DropRelationBuffers (C comment).
        if desc.tag().dbOid != dbid {
            continue;
        }
        let buf_state = LockBufHdr(desc);
        if desc.tag().dbOid == dbid {
            InvalidateBuffer(desc, buf_state)?;
        } else {
            UnlockBufHdr(desc, buf_state);
        }
    }
    Ok(())
}

pub fn DropRelationsAllBuffers(smgr_reln: &[RelFileLocatorBackend]) -> PgResult<()> {
    let mut rels: Vec<RelFileLocatorBackend> = Vec::with_capacity(smgr_reln.len());
    for r in smgr_reln {
        if r.backend != types_core::INVALID_PROC_NUMBER {
            if r.backend == init_small::globals::MyProcNumber() {
                crate::localbuf::DropRelationAllLocalBuffers(r.locator)?;
            }
        } else {
            rels.push(*r);
        }
    }
    if rels.is_empty() {
        return Ok(());
    }

    let nforks = MAX_FORKNUM as usize + 1;
    let mut block: Vec<[BlockNumber; 4]> = vec![[InvalidBlockNumber; 4]; rels.len()];
    debug_assert!(nforks == 4);
    let mut n_blocks_to_invalidate: u64 = 0;
    let mut cached = true;
    'outer: for (i, r) in rels.iter().enumerate() {
        for j in 0..nforks {
            let fork = ForkNumber::from_i32(j as i32).expect("fork number");
            let nblocks = smgr_seams::smgr_nblocks_cached::call(*r, fork);
            block[i][j] = nblocks;
            if nblocks == InvalidBlockNumber {
                if !smgr_seams::smgr_exists::call(*r, fork)? {
                    continue;
                }
                cached = false;
                break 'outer;
            }
            n_blocks_to_invalidate += nblocks as u64;
        }
    }

    if cached && n_blocks_to_invalidate < buf_drop_full_scan_threshold() {
        for (i, r) in rels.iter().enumerate() {
            for j in 0..nforks {
                if block[i][j] == InvalidBlockNumber {
                    continue;
                }
                let fork = ForkNumber::from_i32(j as i32).expect("fork number");
                FindAndDropRelationBuffers(r.locator, fork, block[i][j], 0)?;
            }
        }
        return Ok(());
    }

    let mut locators: Vec<RelFileLocator> = rels.iter().map(|r| r.locator).collect();
    let use_bsearch = locators.len() > RELS_BSEARCH_THRESHOLD;
    if use_bsearch {
        locators.sort_by_key(|l| (l.spcOid, l.dbOid, l.relNumber));
    }

    for i in 0..NBuffersInited() {
        let desc = GetBufferDescriptor(i);
        let tag = desc.tag();
        let rlocator = if use_bsearch {
            let probe = RelFileLocator::new(tag.spcOid, tag.dbOid, tag.relNumber);
            locators
                .binary_search_by_key(&(probe.spcOid, probe.dbOid, probe.relNumber), |l| {
                    (l.spcOid, l.dbOid, l.relNumber)
                })
                .ok()
                .map(|idx| locators[idx])
        } else {
            locators.iter().find(|l| tag_matches(&tag, l)).copied()
        };
        let Some(rlocator) = rlocator else { continue };

        let buf_state = LockBufHdr(desc);
        if tag_matches(&desc.tag(), &rlocator) {
            InvalidateBuffer(desc, buf_state)?;
        } else {
            UnlockBufHdr(desc, buf_state);
        }
    }
    Ok(())
}
