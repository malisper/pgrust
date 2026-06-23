//! Seam declarations for the TID-bitmap iterator/lifecycle routines
//! (`nodes/tidbitmap.c`, the `backend-nodes-core` unit).
//!
//! The bitmap-scan executor builds and iterates the `TIDBitmap` its child
//! subplan produced. tidbitmap.c is not ported yet; a call panics loudly until
//! it lands. The `dsa_area *` C threads for the shared (parallel) iterators is
//! the executor's `es_query_dsa`, modeled as the live [`DsaAreaHandle`].

#![allow(non_snake_case)]

use mcx::{Mcx, PgBox};
use ::types_error::PgResult;
use ::execparallel::DsaAreaHandle;
use tidbitmap::{dsa_pointer, TBMIterateOutcome, TBMIterator, TIDBitmap};

seam_core::seam!(
    /// `tbm_create(maxbytes, dsa)` (tidbitmap.c): create an initially-empty
    /// bitmap usable for up to `maxbytes` of memory. A non-`None` `dsa` makes
    /// the bitmap DSA-shareable (the parallel path; the C passes
    /// `estate->es_query_dsa` when the `BitmapOr` plan `isshared`). The new
    /// bitmap is allocated through the supplied query context (the C palloc), so
    /// the call is boxed into `mcx` and is fallible on OOM.
    pub fn tbm_create<'mcx>(
        mcx: Mcx<'mcx>,
        maxbytes: usize,
        dsa: Option<DsaAreaHandle>,
    ) -> PgResult<PgBox<'mcx, TIDBitmap>>
);

seam_core::seam!(
    /// `tbm_union(a, b)` (tidbitmap.c): `a = a ∪ b` — fold `b` into `a` in
    /// place. The caller frees `b` afterwards (`tbm_free`). Fallible (the C
    /// can `ereport(ERROR)` on a lossy/exact page-table growth allocation).
    pub fn tbm_union(a: &mut TIDBitmap, b: &TIDBitmap) -> PgResult<()>
);

seam_core::seam!(
    /// `tbm_prepare_shared_iterate(tbm)` (tidbitmap.c): prepare the bitmap for
    /// shared iteration across parallel workers, returning the `dsa_pointer` of
    /// the shared iterator state. Allocates in the DSA, so fallible on OOM.
    pub fn tbm_prepare_shared_iterate(tbm: &mut TIDBitmap) -> PgResult<dsa_pointer>
);

seam_core::seam!(
    /// `tbm_begin_iterate(tbm, dsa, dsp)` (tidbitmap.c): begin iterating the
    /// bitmap. With a valid `dsp` (parallel) it attaches the shared iterator in
    /// `dsa`; otherwise it builds a private iterator. Allocates, so fallible on
    /// OOM. `tbm` mirrors the C nullable `TIDBitmap *`: it is only dereferenced
    /// on the private (`dsp` invalid) path, so a non-leader parallel worker that
    /// never built a local bitmap passes `None`.
    pub fn tbm_begin_iterate(
        tbm: Option<&mut TIDBitmap>,
        dsa: Option<DsaAreaHandle>,
        dsp: dsa_pointer,
    ) -> PgResult<TBMIterator>
);

seam_core::seam!(
    /// `tbm_iterate(iterator, tbmres)` (tidbitmap.c): advance the unified
    /// iterator one step, returning the next page's [`TBMIterateOutcome`]
    /// (combining `tbm_iterate` with `tbm_extract_page_tuple` for an exact
    /// page), or `None` when the bitmap is exhausted (the C `false` return /
    /// `blockno == InvalidBlockNumber`). The bitmap-scan table-AM
    /// (`BitmapHeapScanNextBlock`) drives this off the scan descriptor's
    /// `rs_tbmiterator`.
    pub fn tbm_iterate(iterator: &mut TBMIterator) -> PgResult<Option<TBMIterateOutcome>>
);

seam_core::seam!(
    /// `tbm_end_iterate(iterator)` (tidbitmap.c): release the iterator's
    /// resources and NULL out the iterator's pointers (so `tbm_exhausted`
    /// reports done).
    pub fn tbm_end_iterate(iterator: &mut TBMIterator)
);

seam_core::seam!(
    /// `tbm_free(tbm)` (tidbitmap.c): free the bitmap and any buffers it holds.
    pub fn tbm_free(tbm: &mut TIDBitmap)
);

seam_core::seam!(
    /// `tbm_intersect(a, b)` (tidbitmap.c): set `a = a ∩ b` (a modified in
    /// place). Used by `MultiExecBitmapAnd` to AND child subplan bitmaps. `a` is
    /// the real `TIDBitmap *` the owner mutates in place.
    pub fn tbm_intersect(a: &mut TIDBitmap, b: &TIDBitmap) -> PgResult<()>
);

seam_core::seam!(
    /// `tbm_is_empty(tbm)` (tidbitmap.c): report whether the bitmap is empty
    /// (the `MultiExecBitmapAnd` early-out check).
    pub fn tbm_is_empty(tbm: &TIDBitmap) -> PgResult<bool>
);

seam_core::seam!(
    /// `tbm_free_shared_area(dsa, dp)` (tidbitmap.c): free a shared iterator
    /// state DSA allocation made by `tbm_prepare_shared_iterate`.
    pub fn tbm_free_shared_area(dsa: DsaAreaHandle, dp: dsa_pointer)
);
