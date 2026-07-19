//! blinsert.c (insert half): blinsert. The build half lives in the
//! bloom_build crate (execindexing sits above indexam in the crate graph;
//! same split as pgvector_hnsw_build).

use crate::state::{
    bloom_form_tuple, bloom_new_buffer, buf_page_bytes, init_bloom_state,
    GENERIC_XLOG_FULL_IMAGE,
};
use bufmgr::{
    BufferGetBlockNumber, LockBuffer, ReleaseBuffer, UnlockReleaseBuffer, BUFFER_LOCK_EXCLUSIVE,
    BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK,
};
use datum::Datum;
use generic_xlog::{GenericXLogAbort, GenericXLogFinish, GenericXLogStart};
use mcx::Mcx;
use types_bloom::*;
use types_core::{BlockNumber, InvalidBlockNumber};
use types_error::{PgError, PgResult};
use types_rel::Relation;
use types_tuple::itemptr::ItemPointerData;

/// blinsert. Returns false always (no uniqueness).
pub fn blinsert<'mcx>(
    _mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    values: &[Datum],
    isnull: &[bool],
    ht_ctid: &ItemPointerData,
    _heap_rel: &Relation<'mcx>,
) -> PgResult<bool> {
    // C's insertCtx: per-insert scratch, deleted on exit.
    let insert_ctx = mcx::MemoryContext::new_bump("Bloom insert temporary context");
    let imcx = insert_ctx.mcx();

    let mut blstate = init_bloom_state(index)?;
    let itup = bloom_form_tuple(&mut blstate, ht_ctid, values, isnull)?;
    let size = blstate.size_of_bloom_tuple;

    // At first, try to insert to the first page in the notFullPage array.
    // If successful the meta page stays untouched.
    let meta_buffer = bufmgr::ReadBuffer(index, BLOOM_METAPAGE_BLKNO)?;
    LockBuffer(meta_buffer, BUFFER_LOCK_SHARE)?;
    let mut blkno: BlockNumber = InvalidBlockNumber;
    let (n_start_snap, n_end_snap, first_blkno) = {
        let meta_page = buf_page_bytes(meta_buffer);
        let ns = meta_nstart(meta_page);
        let ne = meta_nend(meta_page);
        let fb = if ne > ns { meta_notfull(meta_page, ns as usize) } else { InvalidBlockNumber };
        (ns, ne, fb)
    };

    if n_end_snap > n_start_snap {
        blkno = first_blkno;
        debug_assert!(blkno != InvalidBlockNumber);
        // Don't hold the metabuffer lock while inserting.
        LockBuffer(meta_buffer, BUFFER_LOCK_UNLOCK)?;

        let buffer = bufmgr::ReadBuffer(index, blkno)?;
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE)?;

        let mut state = GenericXLogStart(imcx, index)?;
        let page = state.register_buffer(buffer, 0)?;

        // A page recently deleted by VACUUM can be reused; reinitialize it.
        if page_is_new(page) || page_is_deleted(page) {
            bloom_init_page(page, 0);
        }

        if page_add_item(page, size, &itup) {
            // Success: apply the change, clean up, and exit.
            GenericXLogFinish(state)?;
            UnlockReleaseBuffer(buffer)?;
            ReleaseBuffer(meta_buffer)?;
            return Ok(false);
        }

        // Didn't fit; try other pages.
        GenericXLogAbort(state);
        UnlockReleaseBuffer(buffer)?;
    } else {
        // No entries in notFullPage.
        LockBuffer(meta_buffer, BUFFER_LOCK_UNLOCK)?;
    }

    // Try other pages in the notFullPage array; nStart will change, so hold
    // the metapage exclusively.
    LockBuffer(meta_buffer, BUFFER_LOCK_EXCLUSIVE)?;

    // nStart might have changed while we didn't have lock.
    let mut n_start = meta_nstart(buf_page_bytes(meta_buffer));

    // Skip the first page if we already tried it above.
    {
        let meta_page = buf_page_bytes(meta_buffer);
        if n_start < meta_nend(meta_page)
            && blkno == meta_notfull(meta_page, n_start as usize)
        {
            n_start += 1;
        }
    }

    // Each iteration tries one notFullPage entry with its own
    // GenericXLogState; the loop leaves a live state registered over the
    // metapage for the fallback new-page case (C's for(;;) shape).
    loop {
        let mut state = GenericXLogStart(imcx, index)?;
        let meta_page = state.register_buffer(meta_buffer, 0)?;

        if n_start >= meta_nend(meta_page) {
            // No more entries in notFullPage: allocate a new page.
            // (Same XXX as C: holds ex-lock on metapage across the extend.)
            let buffer = bloom_new_buffer(index)?;
            let new_blkno = BufferGetBlockNumber(buffer);

            let page = state.register_buffer(buffer, GENERIC_XLOG_FULL_IMAGE)?;
            bloom_init_page(page, 0);
            if !page_add_item(page, size, &itup) {
                return Err(PgError::error(
                    "could not add new bloom tuple to empty page",
                )
                .into());
            }

            // Reset the notFullPage array to contain just this new page.
            let meta_page = state.page_image_mut(0);
            meta_set_nstart(meta_page, 0);
            meta_set_nend(meta_page, 1);
            meta_set_notfull(meta_page, 0, new_blkno);

            GenericXLogFinish(state)?;
            UnlockReleaseBuffer(buffer)?;
            UnlockReleaseBuffer(meta_buffer)?;
            return Ok(false);
        }

        blkno = meta_notfull(meta_page, n_start as usize);
        debug_assert!(blkno != InvalidBlockNumber);

        let buffer = bufmgr::ReadBuffer(index, blkno)?;
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE)?;
        let page = state.register_buffer(buffer, 0)?;

        // Basically same logic as above.
        if page_is_new(page) || page_is_deleted(page) {
            bloom_init_page(page, 0);
        }

        if page_add_item(page, size, &itup) {
            // Success: update nStart in the registered metapage image.
            let meta_page = state.page_image_mut(0);
            meta_set_nstart(meta_page, n_start);
            GenericXLogFinish(state)?;
            UnlockReleaseBuffer(buffer)?;
            UnlockReleaseBuffer(meta_buffer)?;
            return Ok(false);
        }

        // Didn't fit; drop this attempt and move to the next page.
        GenericXLogAbort(state);
        UnlockReleaseBuffer(buffer)?;
        n_start += 1;
    }
}
