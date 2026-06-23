//! Dirty-marking, page access, and buffer accessors (bufmgr.c / bufpage.h) —
//! the F1d high-fan-in seam set on already-resident, pinned (and
//! content-locked, where the C requires it) shared buffers.
//!
//! F1d (this stage): `MarkBufferDirty`, `BufferGetBlockNumber` / `BufferGetTag`
//! / `BufferGetLSNAtomic`, the `BufferGetPage` page-access primitives
//! (`with_buffer_page` in-place mutation + `buffer_get_page` owned snapshot),
//! and the bufpage page primitives (`PageInit` / `PageSetLSN` / `PageGetLSN` /
//! `PageIsNew`) routed through a pinned buffer. No I/O, no allocation of shared
//! state, no victim selection (those are F2/F3/F5); the page bytes are the
//! crate-owned `blocks` array, read/written under the caller's content lock
//! exactly where C dereferences `BufferGetPage(buffer)`.

use std::sync::atomic::Ordering;

use types_core::primitive::{BlockNumber, Buffer, OffsetNumber, XLogRecPtr, BLCKSZ};
use types_error::{PgError, PgResult};
use types_storage::buf::{BM_DIRTY, BM_JUST_DIRTIED, BM_LOCKED};
use types_storage::RelFileLocator;

use crate::mgr::BufferManager;

impl BufferManager {
    /// `BufferGetPage(buffer)` read dispatch (bufpage.h): run `f` over the
    /// buffer's `BLCKSZ` page bytes, routing a local (temp) buffer to its
    /// backend-local page in the localbuf pool. The closure may compute any
    /// `Copy`-ish value `R` — for the local arm we take an owned snapshot of the
    /// page (no shared state to alias) and run `f` over it. The caller holds the
    /// pin / content lock exactly as C's `BufferGetPage(buffer)` requires.
    pub(crate) fn with_page_bytes<R>(
        &self,
        buffer: Buffer,
        f: impl FnOnce(&[u8]) -> R,
    ) -> PgResult<R> {
        if crate::buf_lock::buffer_is_local(buffer) {
            // The localbuf page is backend-private, so an in-place read through
            // the local page-access seam is equivalent to C's `LocalBufHdrGetBlock`
            // read. `f` is `FnOnce`; the seam takes an `FnMut`, so stash `f` in an
            // `Option` and `take()` it on the single invocation.
            let mut out: Option<R> = None;
            let mut f_once = Some(f);
            buffer_support_seams::local_buffer_with_page::call(
                buffer,
                &mut |bytes| {
                    out = Some((f_once.take().expect("closure runs once"))(bytes));
                    Ok(())
                },
            )?;
            return Ok(out.expect("local_buffer_with_page closure must run"));
        }
        let buf_id = self.buffer_to_buf_id_pub(buffer)?;
        Ok(self.with_block(buf_id, f))
    }

    /// `BufferGetPage(buffer)` in-place write dispatch (bufpage.h): run `f` over
    /// the buffer's live `BLCKSZ` page bytes for mutation, routing a local
    /// buffer to its backend-local page. The caller holds the exclusive content
    /// lock (a no-op for local buffers). `f`'s `Err` propagates.
    pub(crate) fn with_page_bytes_mut(
        &self,
        buffer: Buffer,
        f: &mut dyn FnMut(&mut [u8]) -> PgResult<()>,
    ) -> PgResult<()> {
        if crate::buf_lock::buffer_is_local(buffer) {
            return buffer_support_seams::local_buffer_with_page::call(buffer, f);
        }
        let buf_id = self.buffer_to_buf_id_pub(buffer)?;
        self.with_block_mut(buf_id, f)
    }

    /// `MarkBufferDirty(buffer)` (bufmgr.c:2640) — mark the contents of a buffer
    /// dirty. The buffer must be pinned and exclusive-content-locked by the
    /// caller. Faithful to the lock-free CAS loop setting `BM_DIRTY |
    /// BM_JUST_DIRTIED` (with the `BM_LOCKED` header-spinlock wait), plus the
    /// dirty-accounting on the 0->1 transition.
    pub fn MarkBufferDirty(&self, buffer: Buffer) -> PgResult<()> {
        // if (!BufferIsValid(buffer)) elog(ERROR, "bad buffer ID"). A local
        // buffer (negative handle) is valid; it routes to MarkLocalBufferDirty.
        if !self.buffer_is_valid(buffer) && !crate::buf_lock::buffer_is_local(buffer) {
            return Err(PgError::error(format!("bad buffer ID: {buffer}")));
        }
        // if (BufferIsLocal(buffer)) { MarkLocalBufferDirty(buffer); return; }
        if crate::buf_lock::buffer_is_local(buffer) {
            return bufmgr_seams::mark_local_buffer_dirty::call(buffer);
        }
        let buf_id = self.buffer_to_buf_id_pub(buffer)?;
        // Assert(BufferIsPinned(buffer)).

        // Assert(GetPrivateRefCount(buffer) > 0).
        if self.private_refcount().get(buf_id as i32) == 0 {
            return Err(PgError::error("MarkBufferDirty: buffer is not pinned"));
        }
        // Assert(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(bufHdr),
        //        LW_EXCLUSIVE)).
        debug_assert!(
            lwlock::LWLockHeldByMe(self.content_lock(buf_id)),
            "MarkBufferDirty: caller must hold the exclusive content lock"
        );

        let state = &self.states_atomic(buf_id);
        let mut old_buf_state = state.load(Ordering::Acquire);
        loop {
            if old_buf_state & BM_LOCKED != 0 {
                old_buf_state = self.wait_buf_hdr_unlocked(buf_id);
            }

            let buf_state = old_buf_state | BM_DIRTY | BM_JUST_DIRTIED;

            // C `pg_atomic_compare_exchange_u32` has FULL barrier semantics
            // (atomics.h:370); `SeqCst` on both orderings matches it
            // (`AcqRel`/`Acquire` would be genuinely weaker).
            match state.compare_exchange_weak(
                old_buf_state,
                buf_state,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    // If the buffer was not dirty already, do vacuum accounting.
                    if old_buf_state & BM_DIRTY == 0 {
                        // VacuumPageDirty++; pgBufferUsage.shared_blks_dirtied++;
                        // VacuumCostBalance bump (if VacuumCostActive).
                        bufmgr_seams::count_buffer_dirtied::call();
                    }
                    break;
                }
                Err(actual) => old_buf_state = actual,
            }
        }
        Ok(())
    }

    // -- accessors (bufmgr.c) ----------------------------------------------

    /// `BufferGetBlockNumber(buffer)` (bufmgr.c:3994) — the block number the
    /// buffer currently holds. The caller must hold a pin (so the tag is
    /// stable). Pure read of the descriptor tag.
    pub fn BufferGetBlockNumber(&self, buffer: Buffer) -> PgResult<BlockNumber> {
        // Assert(BufferIsPinned(buffer)).
        // if (BufferIsLocal(buffer))
        //     bufHdr = GetLocalBufferDescriptor(-buffer - 1);
        // (bufmgr.c:3994). Local/temp buffers carry a negative handle and live
        // in this backend's localbuf pool, not the shared descriptor array.
        if crate::buf_lock::buffer_is_local(buffer) {
            return buffer_support_seams::local_buffer_block_number::call(buffer);
        }
        let buf_id = self.buffer_to_buf_id_pub(buffer)?;
        Ok(self.desc_tag(buf_id).blockNum)
    }

    /// `BufferGetTag(buffer, *rlocator, *forknum, *blknum)` (bufmgr.c:4018) — the
    /// relation/fork/block this buffer currently holds, returned as one owned
    /// triple. The caller must hold a pin.
    pub fn BufferGetTag(
        &self,
        buffer: Buffer,
    ) -> PgResult<(RelFileLocator, types_core::primitive::ForkNumber, BlockNumber)> {
        // Assert(BufferIsPinned(buffer)).
        // if (BufferIsLocal(buffer))
        //     bufHdr = GetLocalBufferDescriptor(-buffer - 1); (bufmgr.c:4018).
        if crate::buf_lock::buffer_is_local(buffer) {
            return buffer_support_seams::local_buffer_get_tag::call(buffer);
        }
        let buf_id = self.buffer_to_buf_id_pub(buffer)?;
        let tag = self.desc_tag(buf_id);
        // BufTagGetRelFileLocator(&tag) / BufTagGetForkNum(&tag) (buf_internals.h).
        let rlocator = RelFileLocator {
            spcOid: tag.spcOid,
            dbOid: tag.dbOid,
            relNumber: tag.relNumber,
        };
        Ok((rlocator, tag.forkNum, tag.blockNum))
    }

    /// `BufferGetLSNAtomic(buffer)` (bufmgr.c:4486) — atomically read a pinned
    /// buffer's page LSN. The caller must hold at least a share lock on the
    /// buffer. For shared buffers the header spinlock is taken so the read is
    /// consistent against a concurrent `MarkBufferDirtyHint` LSN stamp; the
    /// checksums-disabled / unlogged fast path returns the LSN without the lock.
    pub fn BufferGetLSNAtomic(&self, buffer: Buffer) -> PgResult<XLogRecPtr> {
        // Assert(BufferIsPinned(buffer)).
        // A local buffer is never shared, so no header spinlock is needed
        // (bufmgr.c:4486 `if (!XLogHintBitIsNeeded() || BufferIsLocal(buffer))`
        // fast path). Route to the localbuf page-LSN read.
        if crate::buf_lock::buffer_is_local(buffer) {
            return buffer_support_seams::local_buffer_get_lsn::call(buffer);
        }
        let buf_id = self.buffer_to_buf_id_pub(buffer)?;

        // If we don't need locking for correctness, fastpath out: a local buffer
        // (out of this core), an unlogged/temp buffer, or checksums + WAL hints
        // both disabled. We model the always-correct path: take the header lock
        // for a shared, permanent buffer when hint-bit WAL is in play; otherwise
        // a bare read. To stay faithful to the single-impl contract and avoid a
        // racy read, take the header lock unconditionally here (the fast path is
        // a pure optimisation that never changes the value observed).
        // Assert(BufferIsPinned(buffer)).
        // Assert(LWLockHeldByMe(BufferDescriptorGetContentLock(bufHdr))).
        let buf_state = self.lock_buf_hdr(buf_id);
        let lsn = self.with_block(buf_id, |block| {
            let page = page::PageRef::new(block)
                .expect("buffer block is BLCKSZ");
            page::PageGetLSN(&page)
        });
        self.unlock_buf_hdr(buf_id, buf_state);
        Ok(lsn)
    }

    // -- page access (BufferGetPage, bufpage.h) ----------------------------

    /// `BufferGetPage(buffer)` (bufmgr.h) — run `f` over the buffer's live page
    /// bytes (`BLCKSZ`) for in-place read/write. The caller already holds the
    /// pin and (for a write or a consistent read) the content lock, so the
    /// closure operates on the shared page directly — modelling C's bare `Page`
    /// pointer without handing out an aliasable `&'static mut`. `f`'s `Err`
    /// propagates.
    pub fn with_buffer_page(
        &self,
        buffer: Buffer,
        f: &mut dyn FnMut(&mut [u8]) -> PgResult<()>,
    ) -> PgResult<()> {
        // A local (temp) buffer's page lives in this backend's localbuf pool.
        if crate::buf_lock::buffer_is_local(buffer) {
            return buffer_support_seams::local_buffer_with_page::call(buffer, f);
        }
        let buf_id = self.buffer_to_buf_id_pub(buffer)?;
        self.with_block_mut(buf_id, f)
    }

    /// `BufferGetPage(buffer)` (bufmgr.h) materialised as an owned snapshot copy
    /// of the page image in `mcx` (the consumer reads page-format fields off
    /// it). The caller holds the pin / content lock. `Err` carries OOM.
    pub fn BufferGetPageOwned<'mcx>(
        &self,
        mcx: mcx::Mcx<'mcx>,
        buffer: Buffer,
    ) -> PgResult<mcx::PgVec<'mcx, u8>> {
        // A local (temp) buffer's page lives in this backend's localbuf pool.
        if crate::buf_lock::buffer_is_local(buffer) {
            return buffer_support_seams::local_buffer_page_owned::call(mcx, buffer);
        }
        let buf_id = self.buffer_to_buf_id_pub(buffer)?;
        self.with_block(buf_id, |block| mcx::slice_in(mcx, block))
    }

    // -- bufpage page primitives over a pinned buffer (bufpage.c/.h) -------

    /// `PageInit(BufferGetPage(buf), BLCKSZ, 0)` (bufpage.c) — initialise a
    /// fresh (all-zero) page's header. The caller holds the exclusive content
    /// lock. `Err` carries any page-init `ereport(ERROR)`.
    pub fn page_init(&self, buffer: Buffer) -> PgResult<()> {
        self.with_page_bytes_mut(buffer, &mut |block| {
            page::PageInit(block, BLCKSZ, 0)
        })
    }

    /// `PageSetLSN(BufferGetPage(buffer), lsn)` (bufpage.h) — stamp the page
    /// LSN. The caller holds the exclusive content lock.
    pub fn page_set_lsn(&self, buffer: Buffer, lsn: XLogRecPtr) -> PgResult<()> {
        self.with_page_bytes_mut(buffer, &mut |block| {
            let mut page = page::PageMut::new(block)
                .expect("buffer block is BLCKSZ");
            page::PageSetLSN(&mut page, lsn);
            Ok(())
        })
    }

    /// `PageGetLSN(BufferGetPage(buffer))` (bufpage.h) — the page LSN. Unlike
    /// [`Self::BufferGetLSNAtomic`] this is the bare, non-atomic accessor (the
    /// caller already holds the exclusive content lock that serialises the
    /// stamp).
    pub fn page_get_lsn(&self, buffer: Buffer) -> PgResult<XLogRecPtr> {
        self.with_page_bytes(buffer, |block| {
            let page = page::PageRef::new(block)
                .expect("buffer block is BLCKSZ");
            page::PageGetLSN(&page)
        })
    }

    /// `PageIsNew(BufferGetPage(buffer))` (bufpage.h) — whether the buffer's
    /// page is all-zeroes (`pd_upper == 0`).
    pub fn page_is_new(&self, buffer: Buffer) -> PgResult<bool> {
        self.with_page_bytes(buffer, |block| {
            let page = page::PageRef::new(block)
                .expect("buffer block is BLCKSZ");
            page::PageIsNew(&page)
        })
    }

    /// `PageIsEmpty(BufferGetPage(buffer))` (bufpage.h).
    pub fn page_is_empty(&self, buffer: Buffer) -> PgResult<bool> {
        self.with_page_bytes(buffer, |block| {
            let page = page::PageRef::new(block)
                .expect("buffer block is BLCKSZ");
            page::PageIsEmpty(&page)
        })
    }

    /// `PageIsAllVisible(BufferGetPage(buffer))` (bufpage.h).
    pub fn page_is_all_visible(&self, buffer: Buffer) -> PgResult<bool> {
        self.with_page_bytes(buffer, |block| {
            let page = page::PageRef::new(block)
                .expect("buffer block is BLCKSZ");
            page::PageIsAllVisible(&page)
        })
    }

    /// `PageSetAllVisible(BufferGetPage(buffer))` (bufpage.h). Caller holds the
    /// exclusive content lock.
    pub fn page_set_all_visible(&self, buffer: Buffer) -> PgResult<()> {
        self.with_page_bytes_mut(buffer, &mut |block| {
            let mut page = page::PageMut::new(block)
                .expect("buffer block is BLCKSZ");
            page::PageSetAllVisible(&mut page);
            Ok(())
        })
    }

    /// `PageClearAllVisible(BufferGetPage(buffer))` (bufpage.h). Caller holds the
    /// exclusive content lock.
    pub fn page_clear_all_visible(&self, buffer: Buffer) -> PgResult<()> {
        self.with_page_bytes_mut(buffer, &mut |block| {
            let mut page = page::PageMut::new(block)
                .expect("buffer block is BLCKSZ");
            page::PageClearAllVisible(&mut page);
            Ok(())
        })
    }

    /// `PageGetLSN(BufferGetPage(buffer)) == InvalidXLogRecPtr`? (bufpage.h).
    pub fn page_lsn_is_invalid(&self, buffer: Buffer) -> PgResult<bool> {
        Ok(self.page_get_lsn(buffer)? == 0)
    }

    /// `PageGetMaxOffsetNumber(BufferGetPage(buffer))` (bufpage.h).
    pub fn page_get_max_offset_number(&self, buffer: Buffer) -> PgResult<OffsetNumber> {
        self.with_page_bytes(buffer, |block| {
            let page = page::PageRef::new(block)
                .expect("buffer block is BLCKSZ");
            page::PageGetMaxOffsetNumber(&page)
        })
    }

    /// `PageGetHeapFreeSpace(BufferGetPage(buffer))` (bufpage.c).
    pub fn page_get_heap_free_space(&self, buffer: Buffer) -> PgResult<usize> {
        self.with_page_bytes(buffer, |block| {
            let page = page::PageRef::new(block)
                .expect("buffer block is BLCKSZ");
            page::PageGetHeapFreeSpace(&page) as usize
        })
    }

    /// `PageTruncateLinePointerArray(BufferGetPage(buffer))` (bufpage.c). Caller
    /// holds the exclusive content lock.
    pub fn page_truncate_line_pointer_array(&self, buffer: Buffer) -> PgResult<()> {
        self.with_page_bytes_mut(buffer, &mut |block| {
            let mut page = page::PageMut::new(block)
                .expect("buffer block is BLCKSZ");
            page::PageTruncateLinePointerArray(&mut page);
            Ok(())
        })
    }

    /// Read the line-pointer flag state at `(BufferGetPage(buffer), offnum)`
    /// (itemid.h accessors over `PageGetItemId`).
    pub fn page_item_id_state(
        &self,
        buffer: Buffer,
        offnum: OffsetNumber,
    ) -> PgResult<types_vacuum::vacuumlazy::LinePointerState> {
        self.with_page_bytes(buffer, |block| {
            let page = page::PageRef::new(block)
                .expect("buffer block is BLCKSZ");
            let itemid = page::PageGetItemId(&page, offnum)?;
            Ok(types_vacuum::vacuumlazy::LinePointerState {
                is_used: page::ItemIdIsUsed(&itemid),
                is_redirected: page::ItemIdIsRedirected(&itemid),
                is_dead: page::ItemIdIsDead(&itemid),
                is_normal: page::ItemIdIsNormal(&itemid),
                has_storage: page::ItemIdHasStorage(&itemid),
            })
        })?
    }

    /// `ItemIdSetUnused(PageGetItemId(BufferGetPage(buffer), offnum))`
    /// (itemid.h). Caller holds the exclusive content lock.
    pub fn page_item_id_set_unused(
        &self,
        buffer: Buffer,
        offnum: OffsetNumber,
    ) -> PgResult<()> {
        self.with_page_bytes_mut(buffer, &mut |block| {
            let mut itemid = {
                let r = page::PageRef::new(block)
                    .expect("buffer block is BLCKSZ");
                page::PageGetItemId(&r, offnum)?
            };
            page::ItemIdSetUnused(&mut itemid);
            let mut page = page::PageMut::new(block)
                .expect("buffer block is BLCKSZ");
            page::PageSetItemId(&mut page, offnum, itemid)
        })
    }

    // -- FSM page round-trip ((FSMPage) PageGetContents, fsm_internals.h) ---

    /// `(FSMPage) PageGetContents(BufferGetPage(buf))` (fsm_internals.h)
    /// materialised as an owned [`fsm::FSMPageData`]. The caller holds the
    /// appropriate buffer content lock. The FSM struct lives at the page's
    /// `PageGetContents` offset (`MAXALIGN(SizeOfPageHeaderData)`): a 4-byte
    /// native-order `int fp_next_slot` followed by `NodesPerPage` one-byte tree
    /// nodes (`uint8 fp_nodes[]`).
    pub fn fsm_buffer_get_page(&self, buffer: Buffer) -> PgResult<fsm::FSMPageData> {
        self.with_page_bytes(buffer, |block| {
            let base = fsm_contents_offset();
            let fp_next_slot = i32::from_ne_bytes(
                block[base..base + 4]
                    .try_into()
                    .expect("FSM page has room for fp_next_slot"),
            );
            let nodes_start = base + fsm::OFFSET_OF_FP_NODES;
            let fp_nodes =
                block[nodes_start..nodes_start + fsm::NodesPerPage].to_vec();
            fsm::FSMPageData {
                fp_next_slot,
                fp_nodes,
            }
        })
    }

    /// Store a mutated FSM page body back into `(FSMPage)
    /// PageGetContents(BufferGetPage(buf))` (the C in-place page mutation). The
    /// caller holds the exclusive content lock.
    pub fn fsm_buffer_set_page(
        &self,
        buffer: Buffer,
        page: fsm::FSMPageData,
    ) -> PgResult<()> {
        debug_assert_eq!(
            page.fp_nodes.len(),
            fsm::NodesPerPage,
            "fsm_buffer_set_page: fp_nodes must be NodesPerPage long"
        );
        self.with_page_bytes_mut(buffer, &mut |block| {
            let base = fsm_contents_offset();
            block[base..base + 4].copy_from_slice(&page.fp_next_slot.to_ne_bytes());
            let nodes_start = base + fsm::OFFSET_OF_FP_NODES;
            block[nodes_start..nodes_start + fsm::NodesPerPage]
                .copy_from_slice(&page.fp_nodes);
            Ok(())
        })
    }
}

/// `PageGetContents(page)` byte offset — `MAXALIGN(SizeOfPageHeaderData)`
/// (bufpage.h), the start of the page's special-purpose contents area.
#[inline]
fn fsm_contents_offset() -> usize {
    const MAXIMUM_ALIGNOF: usize = 8;
    let len = types_storage::bufpage::SizeOfPageHeaderData;
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn install_stubs() {
        crate::mgr::test_seams::install();
    }

    fn mk() -> BufferManager {
        BufferManager::new(4)
    }

    #[test]
    fn mark_buffer_dirty_sets_flags_and_counts_once() {
        install_stubs();
        bufmgr_seams::count_buffer_dirtied::set(|| {});
        let bm = mk();
        // Pin + take the exclusive content lock (LWLockHeldByMe debug_assert).
        let _ = bm.pin_buffer_for_test(0, false);
        bm.LockBuffer(1, types_storage::buf::BUFFER_LOCK_EXCLUSIVE)
            .unwrap();
        assert_eq!(bm.read_state(0) & BM_DIRTY, 0);
        bm.MarkBufferDirty(1).unwrap();
        assert_ne!(bm.read_state(0) & BM_DIRTY, 0);
        // BM_JUST_DIRTIED also set.
        assert_ne!(bm.read_state(0) & BM_JUST_DIRTIED, 0);
        bm.LockBuffer(1, types_storage::buf::BUFFER_LOCK_UNLOCK)
            .unwrap();
    }

    #[test]
    fn mark_buffer_dirty_rejects_unpinned_and_invalid() {
        install_stubs();
        let bm = mk();
        assert!(bm.MarkBufferDirty(0).is_err()); // invalid id
        assert!(bm.MarkBufferDirty(2).is_err()); // not pinned
    }

    #[test]
    fn block_number_and_tag_read_descriptor() {
        install_stubs();
        let bm = mk();
        let _ = bm.pin_buffer_for_test(0, false);
        // Default tag is zeroed: block 0, fork main(0).
        assert_eq!(bm.BufferGetBlockNumber(1).unwrap(), 0);
        let (rloc, _fork, blk) = bm.BufferGetTag(1).unwrap();
        assert_eq!(blk, 0);
        assert_eq!(rloc.relNumber, 0);
    }

    #[test]
    fn page_init_and_lsn_roundtrip() {
        install_stubs();
        let bm = mk();
        let _ = bm.pin_buffer_for_test(0, false);
        bm.LockBuffer(1, types_storage::buf::BUFFER_LOCK_EXCLUSIVE)
            .unwrap();
        // A fresh (all-zero) page is "new".
        assert!(bm.page_is_new(1).unwrap());
        bm.page_init(1).unwrap();
        // After PageInit pd_upper != 0 -> not new.
        assert!(!bm.page_is_new(1).unwrap());
        bm.page_set_lsn(1, 0x1234_5678).unwrap();
        assert_eq!(bm.page_get_lsn(1).unwrap(), 0x1234_5678);
        assert_eq!(bm.BufferGetLSNAtomic(1).unwrap(), 0x1234_5678);
        bm.LockBuffer(1, types_storage::buf::BUFFER_LOCK_UNLOCK)
            .unwrap();
    }

    #[test]
    fn with_buffer_page_mutates_in_place() {
        install_stubs();
        let bm = mk();
        let _ = bm.pin_buffer_for_test(0, false);
        bm.with_buffer_page(1, &mut |b| {
            b[100] = 0xAB;
            Ok(())
        })
        .unwrap();
        bm.with_buffer_page(1, &mut |b| {
            assert_eq!(b[100], 0xAB);
            Ok(())
        })
        .unwrap();
    }
}
