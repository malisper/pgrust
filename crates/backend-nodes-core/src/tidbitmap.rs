//! Family: **tidbitmap** — `nodes/tidbitmap.c`, the `TIDBitmap` used by bitmap
//! index/heap scans.
//!
//! A `TIDBitmap` maps block numbers to (exact or lossy) per-page tuple-offset
//! bitmaps, with a private hash table (`pagetable`), lossification under a
//! work-mem budget, union/intersect, and private + DSA-shared iterators. The
//! concrete `TIDBitmap`/`TBMIterator` carriers already live in
//! `types_tidbitmap` (authored by nodeBitmapHeapscan).
//!
//! Owns the canonical seams the bitmap-scan executor already calls:
//! `backend-nodes-core-tidbitmap-seams` (`tbm_prepare_shared_iterate`,
//! `tbm_begin_iterate`, `tbm_end_iterate`, `tbm_free`, `tbm_free_shared_area`)
//! and the `tbm_add_tuple` seam in `backend-nodes-core-seams`. Installed in
//! `init_seams()` when this family is filled.
//!
//! Independent of the keystone (own carrier type); deps: backend-utils-error
//! (work-mem ereport), the DSA owner for shared iterators. Skeleton: the full
//! ~40-function machinery lands when filled.

#![allow(unused)]

/// Family marker — the tidbitmap machinery lands here. See module docs.
pub fn tidbitmap_family_unimplemented() -> ! {
    todo!("tidbitmap: nodes/tidbitmap.c not yet ported (decomp family)")
}
