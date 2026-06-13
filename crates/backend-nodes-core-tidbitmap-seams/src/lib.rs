//! Seam declarations for the TID-bitmap iterator/lifecycle routines
//! (`nodes/tidbitmap.c`, the `backend-nodes-core` unit).
//!
//! The bitmap-scan executor builds and iterates the `TIDBitmap` its child
//! subplan produced. tidbitmap.c is not ported yet; a call panics loudly until
//! it lands. The `dsa_area *` C threads for the shared (parallel) iterators is
//! the executor's `es_query_dsa`, modeled as the live [`DsaAreaHandle`].

#![allow(non_snake_case)]

use types_error::PgResult;
use types_execparallel::DsaAreaHandle;
use types_tidbitmap::{dsa_pointer, TBMIterator, TIDBitmap};

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
    /// OOM.
    pub fn tbm_begin_iterate(
        tbm: &mut TIDBitmap,
        dsa: Option<DsaAreaHandle>,
        dsp: dsa_pointer,
    ) -> PgResult<TBMIterator>
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
    /// `tbm_free_shared_area(dsa, dp)` (tidbitmap.c): free a shared iterator
    /// state DSA allocation made by `tbm_prepare_shared_iterate`.
    pub fn tbm_free_shared_area(dsa: DsaAreaHandle, dp: dsa_pointer)
);
