//! The buffer-drop / discard path (relation truncate, relation/db drop), ported
//! from `src/backend/storage/buffer/bufmgr.c`:
//!
//!  * `DropRelationBuffers` (4426) — drop the buffers of one relation at/after a
//!    per-fork truncation point.
//!  * `DropRelationsAllBuffers` (4656) — drop all buffers for a set of relations.
//!  * `FindAndDropRelationBuffers` (4827) — find + invalidate the buffers of one
//!    fork at/after a target block, via the BufMapping hash (the cached-size fast
//!    path).
//!
//! These reuse the header-spinlock / mapping-lock / `invalidate_buffer`
//! machinery owned by [`crate::bufalloc`] and the landed
//! [`smgr`] fork-size accessors. The per-buffer drop holds
//! no content lock (the page is discarded, not written); the mapping partition
//! lock is taken directly via the lwlock dep.
//!
//! `START_CRIT_SECTION()` / `END_CRIT_SECTION()` (the `CritSectionCount`
//! interrupt-holdoff bracket) are elided here exactly as elsewhere in this core:
//! they are not a checkpoint delay, carry no shared-state mutation, and there is
//! no `CritSectionCount` seam.
#![allow(dead_code)]

use ::types_core::primitive::{BlockNumber, ForkNumber, InvalidBlockNumber};
use ::types_error::{PgError, PgResult};
use ::types_storage::buf::buftag;
use ::types_storage::storage::LWLockMode;
use ::types_storage::{RelFileLocator, RelFileLocatorBackend};

use ::support::{buf_table_hash_code, buf_table_hash_partition};

use crate::mgr::BufferManager;

/// `RELS_BSEARCH_THRESHOLD` (bufmgr.c:83) — above this many relations, switch the
/// per-buffer membership test from a linear walk to `bsearch`.
const RELS_BSEARCH_THRESHOLD: usize = 20;

/// `MAX_FORKNUM + 1` — the number of forks per relation. `MAX_FORKNUM` is
/// `INIT_FORKNUM` (discriminant 3), so there are 4 forks (`MAIN`/`FSM`/`VM`/
/// `INIT`), each addressed by its 0-based fork discriminant.
const NFORKS: usize = 4;

/// `BlockNumberIsValid(blockNumber)` (block.h).
#[inline]
fn block_number_is_valid(block_number: BlockNumber) -> bool {
    block_number != InvalidBlockNumber
}

/// The fork named by its 0-based index `j` in `0..NFORKS` (the C `for (forkNum =
/// 0; forkNum <= MAX_FORKNUM; forkNum++)` loop variable).
#[inline]
fn fork_of(j: usize) -> ForkNumber {
    ForkNumber::from_i32(j as i32).expect("fork index in 0..NFORKS")
}

/// `BufTagGetRelFileLocator(tag)` — recover the `RelFileLocator` a buffer tag
/// names (the key of the `bsearch` membership test).
#[inline]
fn tag_to_rlocator(tag: &buftag) -> RelFileLocator {
    RelFileLocator {
        spcOid: tag.spcOid,
        dbOid: tag.dbOid,
        relNumber: tag.relNumber,
    }
}

/// `RelFileLocatorEquals` over a buffer tag's relfilelocator part.
#[inline]
fn tag_matches_rlocator(tag: &buftag, rlocator: &RelFileLocator) -> bool {
    tag.spcOid == rlocator.spcOid
        && tag.dbOid == rlocator.dbOid
        && tag.relNumber == rlocator.relNumber
}

/// `InitBufferTag(&tag, rlocator, forkNum, blockNum)` — the unbacked (shared)
/// tag for a block (temp buffers are localbuf.c's, out of this core).
#[inline]
fn make_tag(rlocator: RelFileLocator, forknum: ForkNumber, blocknum: BlockNumber) -> buftag {
    buftag {
        spcOid: rlocator.spcOid,
        dbOid: rlocator.dbOid,
        relNumber: rlocator.relNumber,
        forkNum: forknum,
        blockNum: blocknum,
    }
}

impl BufferManager {
    // -----------------------------------------------------------------------
    // DropRelationBuffers (bufmgr.c:4426)
    // -----------------------------------------------------------------------

    /// `DropRelationBuffers(smgr_reln, forkNum, nforks, firstDelBlock)`
    /// (bufmgr.c:4426) — remove from the shared buffer pool all pages of
    /// `smgr_reln`'s forks `forkNum[i]` whose block number is `>=
    /// firstDelBlock[i]`, without writing the contents. `smgrtruncate` calls it
    /// before truncating the relation on disk.
    ///
    /// Temp relations are localbuf.c's problem (filtered here); the shared pool
    /// holds none of their buffers, so the local arm routes through the localbuf
    /// seam.
    pub fn DropRelationBuffers(
        &self,
        smgr_reln: RelFileLocatorBackend,
        fork_num: &[ForkNumber],
        first_del_block: &[BlockNumber],
    ) -> PgResult<()> {
        let nforks = fork_num.len();
        debug_assert_eq!(nforks, first_del_block.len());

        // If it's a local relation, it's localbuf.c's problem (bufmgr.c:4441).
        // C loops `DropRelationLocalBuffers(rlocator, forkNum[j], firstDelBlock[j])`
        // once per fork; the support `DropRelationLocalBuffers` takes the whole
        // `forkNum[]`/`firstDelBlock[]` slices in one scan, so the seam carries
        // them directly (installed by the localbuf owner — panic-until-owner).
        if smgr_reln.backend != ::types_core::primitive::INVALID_PROC_NUMBER {
            if smgr_reln.backend == lmgr_proc_seams::my_proc_number::call() {
                bufmgr_seams::drop_relation_local_buffers::call(
                    smgr_reln.locator,
                    fork_num,
                    first_del_block,
                )?;
            }
            return Ok(());
        }

        // We can avoid scanning the entire buffer pool if we know the exact size
        // of each of the given relation forks. See DropRelationsAllBuffers
        // (bufmgr.c:4470).
        let mut cached = true;
        let mut n_blocks_to_invalidate: u64 = 0;
        let mut n_fork_block = [InvalidBlockNumber; NFORKS];

        for j in 0..nforks {
            // Get the number of blocks for a relation's fork (bufmgr.c:4476).
            let nblocks = smgr::smgrnblocks_cached(smgr_reln, fork_num[j]);
            if nblocks == InvalidBlockNumber {
                cached = false;
                break;
            }

            // We only need to invalidate the pages at or after firstDelBlock.
            n_fork_block[j] = nblocks;
            n_blocks_to_invalidate += (nblocks - first_del_block[j]) as u64;
        }

        // We apply the optimization iff the total number of blocks to invalidate
        // is below the BUF_DROP_FULL_SCAN_THRESHOLD ((uint64)(NBuffers / 32))
        // (bufmgr.c:4493).
        let buf_drop_full_scan_threshold = (self.nbuffers() / 32) as u64;
        if cached && n_blocks_to_invalidate < buf_drop_full_scan_threshold {
            for j in 0..nforks {
                self.find_and_drop_relation_buffers(
                    smgr_reln.locator,
                    fork_num[j],
                    n_fork_block[j],
                    first_del_block[j],
                )?;
            }
            return Ok(());
        }

        // Otherwise, a full scan of the buffer pool (bufmgr.c:4506).
        for buf_id in 0..self.nbuffers() as usize {
            // An unlocked precheck should be safe and saves some cycles
            // (bufmgr.c:4516).
            let buf_tag = self.desc_tag(buf_id);
            if !tag_matches_rlocator(&buf_tag, &smgr_reln.locator) {
                continue;
            }

            let buf_state = self.lock_buf_hdr(buf_id);
            let buf_tag = self.desc_tag(buf_id);

            let mut should_drop = false;
            for j in 0..nforks {
                if tag_matches_rlocator(&buf_tag, &smgr_reln.locator)
                    && buf_tag.forkNum == fork_num[j]
                    && buf_tag.blockNum >= first_del_block[j]
                {
                    should_drop = true;
                    break;
                }
            }

            if should_drop {
                self.invalidate_buffer(buf_id, buf_state)?; // releases spinlock
            } else {
                self.unlock_buf_hdr(buf_id, buf_state);
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // DropRelationsAllBuffers (bufmgr.c:4656)
    // -----------------------------------------------------------------------

    /// `DropRelationsAllBuffers(smgr_reln, nlocators)` (bufmgr.c:4656) — drop
    /// every shared buffer belonging to any of the given relations. Local (temp)
    /// relations are filtered out (localbuf.c's problem). When every relation's
    /// fork sizes are known cheaply and the total blocks fall below
    /// `BUF_DROP_FULL_SCAN_THRESHOLD`, the per-fork hash-lookup fast path
    /// ([`Self::find_and_drop_relation_buffers`]) is used; otherwise a full scan
    /// of the pool runs.
    pub fn DropRelationsAllBuffers(&self, smgr_reln: &[RelFileLocatorBackend]) -> PgResult<()> {
        let nlocators = smgr_reln.len();
        if nlocators == 0 {
            return Ok(());
        }

        let mut n = 0usize;
        let mut n_blocks_to_invalidate: u64 = 0;
        let mut cached = true;

        // rels = palloc(...) — the non-local relations (bufmgr.c:4670).
        let mut rels: Vec<RelFileLocatorBackend> = Vec::new();
        rels.try_reserve(nlocators)
            .map_err(|_| PgError::error("DropRelationsAllBuffers: out of memory"))?;

        // If it's a local relation, it's localbuf.c's problem (bufmgr.c:4676).
        for reln in smgr_reln.iter() {
            if reln.backend != ::types_core::primitive::INVALID_PROC_NUMBER {
                if reln.backend == lmgr_proc_seams::my_proc_number::call() {
                    bufmgr_seams::drop_relation_all_local_buffers::call(
                        reln.locator,
                    )?;
                }
            } else {
                rels.push(*reln);
                n += 1;
            }
        }

        // If there are no non-local relations, then we're done (bufmgr.c:4692).
        if n == 0 {
            return Ok(());
        }

        // block[i][j] — number of blocks for all the relations' forks
        // (bufmgr.c:4700).
        let mut block: Vec<[BlockNumber; NFORKS]> = Vec::new();
        block
            .try_reserve(n)
            .map_err(|_| PgError::error("DropRelationsAllBuffers: out of memory"))?;
        block.resize(n, [InvalidBlockNumber; NFORKS]);

        // We can avoid scanning the entire buffer pool if we know the exact size
        // of each of the given relation forks (bufmgr.c:4707).
        let mut i = 0usize;
        'outer: while i < n {
            for j in 0..NFORKS {
                let forknum = fork_of(j);
                // Get the number of blocks for a relation's fork (bufmgr.c:4713).
                let nb = smgr::smgrnblocks_cached(rels[i], forknum);
                block[i][j] = nb;

                // We need to consider only the relation forks that exist.
                if nb == InvalidBlockNumber {
                    if !smgr::smgrexists(rels[i], forknum)? {
                        continue;
                    }
                    cached = false;
                    break 'outer;
                }

                // calculate the total number of blocks to be invalidated.
                n_blocks_to_invalidate += nb as u64;
            }
            i += 1;
        }

        // We apply the optimization iff the total number of blocks to invalidate
        // is below the BUF_DROP_FULL_SCAN_THRESHOLD (bufmgr.c:4736).
        let buf_drop_full_scan_threshold = (self.nbuffers() / 32) as u64;
        if cached && n_blocks_to_invalidate < buf_drop_full_scan_threshold {
            for i in 0..n {
                for j in 0..NFORKS {
                    // ignore relation forks that don't exist.
                    if !block_number_is_valid(block[i][j]) {
                        continue;
                    }
                    // drop all the buffers for a particular relation fork.
                    self.find_and_drop_relation_buffers(
                        rels[i].locator,
                        fork_of(j),
                        block[i][j],
                        0,
                    )?;
                }
            }
            return Ok(());
        }

        // locators = palloc(...) — non-local relations (bufmgr.c:4757).
        let mut locators: Vec<RelFileLocator> = Vec::new();
        locators
            .try_reserve(n)
            .map_err(|_| PgError::error("DropRelationsAllBuffers: out of memory"))?;
        for rel in rels.iter().take(n) {
            locators.push(rel.locator);
        }

        // For low number of relations to drop just use a simple walk through, to
        // save the bsearch overhead (bufmgr.c:4768).
        let use_bsearch = n > RELS_BSEARCH_THRESHOLD;

        // sort the list of rlocators if necessary (bufmgr.c:4773).
        if use_bsearch {
            locators.sort_by(crate::buf_flush::rlocator_comparator);
        }

        for buf_id in 0..self.nbuffers() as usize {
            // As in DropRelationBuffers, an unlocked precheck should be safe and
            // saves some cycles (bufmgr.c:4782).
            let buf_tag = self.desc_tag(buf_id);
            let matched: Option<RelFileLocator> = if !use_bsearch {
                let mut found = None;
                for loc in locators.iter().take(n) {
                    if tag_matches_rlocator(&buf_tag, loc) {
                        found = Some(*loc);
                        break;
                    }
                }
                found
            } else {
                let locator = tag_to_rlocator(&buf_tag);
                match locators
                    .binary_search_by(|probe| crate::buf_flush::rlocator_comparator(probe, &locator))
                {
                    Ok(idx) => Some(locators[idx]),
                    Err(_) => None,
                }
            };

            // buffer doesn't belong to any of the given relfilelocators; skip it.
            let rlocator = match matched {
                Some(loc) => loc,
                None => continue,
            };

            let buf_state = self.lock_buf_hdr(buf_id);
            if tag_matches_rlocator(&self.desc_tag(buf_id), &rlocator) {
                self.invalidate_buffer(buf_id, buf_state)?; // releases spinlock
            } else {
                self.unlock_buf_hdr(buf_id, buf_state);
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // FindAndDropRelationBuffers (bufmgr.c:4827)
    // -----------------------------------------------------------------------

    /// `FindAndDropRelationBuffers(rlocator, forkNum, nForkBlock, firstDelBlock)`
    /// (bufmgr.c:4827) — remove from the buffer pool all pages of `rlocator`'s
    /// `forkNum` whose block number is `>= firstDelBlock`, via the BufMapping
    /// hash. (`firstDelBlock = 0` drops the whole fork.)
    pub(crate) fn find_and_drop_relation_buffers(
        &self,
        rlocator: RelFileLocator,
        fork_num: ForkNumber,
        n_fork_block: BlockNumber,
        first_del_block: BlockNumber,
    ) -> PgResult<()> {
        let mut cur_block = first_del_block;
        while cur_block < n_fork_block {
            // create a tag so we can lookup the buffer (bufmgr.c:4838).
            let buf_tag = make_tag(rlocator, fork_num, cur_block);

            // determine its hash code and partition lock ID (bufmgr.c:4842).
            let buf_hash = buf_table_hash_code(&buf_tag);
            let part = buf_table_hash_partition(buf_hash);

            // Check that it is in the buffer pool. If not, do nothing
            // (bufmgr.c:4846).
            let guard = self.map_acquire(part, LWLockMode::LW_SHARED)?;
            let buf_id = self.buf_table().lookup(&buf_tag, buf_hash);
            guard.release()?;

            if buf_id < 0 {
                cur_block = cur_block.wrapping_add(1);
                continue;
            }

            let buf_id = buf_id as usize;

            // We need to lock the buffer header and recheck if the buffer is
            // still associated with the same block because the buffer could be
            // evicted by some other backend loading blocks for a different
            // relation after we release lock on the BufMapping table
            // (bufmgr.c:4862).
            let buf_state = self.lock_buf_hdr(buf_id);
            let hdr_tag = self.desc_tag(buf_id);

            if tag_matches_rlocator(&hdr_tag, &rlocator)
                && hdr_tag.forkNum == fork_num
                && hdr_tag.blockNum >= first_del_block
            {
                self.invalidate_buffer(buf_id, buf_state)?; // releases spinlock
            } else {
                self.unlock_buf_hdr(buf_id, buf_state);
            }

            cur_block = cur_block.wrapping_add(1);
        }
        Ok(())
    }

    /// `DropDatabaseBuffers(dbid)` (bufmgr.c:4888) — remove from the shared
    /// buffer pool every page belonging to database `dbid`. Dirty pages are
    /// dropped without being written (used when the database directory tree is
    /// being destroyed). Local buffers need not be considered — by assumption
    /// the target database is not our own.
    pub fn DropDatabaseBuffers(&self, dbid: ::types_core::Oid) -> PgResult<()> {
        for buf_id in 0..self.nbuffers() as usize {
            // As in DropRelationBuffers, an unlocked precheck should be safe and
            // saves some cycles.
            let buf_tag = self.desc_tag(buf_id);
            if buf_tag.dbOid != dbid {
                continue;
            }

            let buf_state = self.lock_buf_hdr(buf_id);
            let buf_tag = self.desc_tag(buf_id);
            if buf_tag.dbOid == dbid {
                self.invalidate_buffer(buf_id, buf_state)?; // releases spinlock
            } else {
                self.unlock_buf_hdr(buf_id, buf_state);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_number_is_valid_matches_c() {
        assert!(!block_number_is_valid(InvalidBlockNumber));
        assert!(block_number_is_valid(0));
        assert!(block_number_is_valid(42));
    }

    #[test]
    fn nforks_and_thresholds_match_c() {
        assert_eq!(RELS_BSEARCH_THRESHOLD, 20);
        assert_eq!(NFORKS, 4);
    }

    #[test]
    fn tag_to_rlocator_roundtrips() {
        let t = make_tag(
            RelFileLocator {
                spcOid: 1664,
                dbOid: 5,
                relNumber: 16384,
            },
            ForkNumber::MAIN_FORKNUM,
            7,
        );
        let back = tag_to_rlocator(&t);
        assert_eq!(back.spcOid, 1664);
        assert_eq!(back.dbOid, 5);
        assert_eq!(back.relNumber, 16384);
    }
}
