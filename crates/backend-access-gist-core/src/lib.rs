//! `backend-access-gist-core` — owned-tree Rust port of the GiST index access
//! method's core (`src/backend/access/gist/`), PostgreSQL 18.3.
//!
//! This stage (F1-utils) ports the GiST utility slice that the scan / insert /
//! build layers all rest on:
//!
//!   * `gist.c` — `initGISTstate` (build the per-column opclass support-proc
//!     dispatch `GISTSTATE` + the leaf / truncated-non-leaf tuple descriptors,
//!     cached on the index relation via `rd_amcache`).
//!   * `gistutil.c` — `gistFormTuple` / `gistCompressValues` (compress + form an
//!     index tuple), the `gistdentryinit` decompress core, the page-byte
//!     primitives `gistinitpage` / `GISTInitBuffer` / `gistcheckpage` /
//!     `gistfillbuffer`, and the `GISTPageOpaqueData` special-area accessors
//!     (`GistPageGetNSN` / `GistPageSetNSN` / rightlink / flags).
//!
//! GiST dispatches its opclass support procedures
//! (`consistent`/`union`/`compress`/`decompress`/`penalty`/`picksplit`/
//! `same`/`distance`/`fetch`) through the per-opclass *typed* seams in
//! `backend-access-gist-dispatch-seams` (installed by `backend-access-gist-proc`
//! for the box/point opclass), keyed on the support-procedure OID resolved by
//! `index_getprocinfo` / `index_getprocid` — not a generic fmgr-by-pointer
//! path. The page bytes are reached through the bufmgr seam
//! (`with_buffer_page`) and the page-format primitives in
//! `backend-storage-page`, exactly like `backend-access-brin-pageops`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![no_std]

extern crate alloc;

pub mod gist_insert;
pub mod gist_page;
pub mod gist_scan;
pub mod gist_vacuum;
pub mod gistsplit;
pub mod gistutil;

pub use gist_insert::{gistSplit, gistdoinsert, gistplacetopage, gistprunepage};
pub use gist_page::{
    gist_page_flags, gist_page_get_nsn, gist_page_rightlink, gist_page_set_nsn, gistcheckpage,
    gistfillbuffer, gistinitpage, set_gist_page_flags, set_gist_page_rightlink, GISTInitBuffer,
    GiSTPageSize, GistClearFollowRight, GistClearPageHasGarbage, GistFollowRight, GistMarkFollowRight,
    GistMarkPageHasGarbage, GistPageGetDeleteXid, GistPageHasGarbage, GistPageIsDeleted,
    GistPageIsLeaf,
};
pub use gist_scan::{
    gistbeginscan, gistcanreturn, gistendscan, gistgetbitmap, gistgettuple, gisthandler,
    gistrescan,
};
pub use gist_vacuum::{gistbulkdelete, gistvacuumcleanup};
pub use gistsplit::gistSplitByKey;
pub use gistutil::{
    gistCompressValues, gistDeCompressAtt, gistFetchTuple, gistFormTuple, gistKeyIsEQ,
    gistMakeUnionItVec, gistMakeUnionKey, gistNewBuffer, gist_page_recyclable,
    gist_translate_cmptype_common, gistchoose, gistdentryinit, gistextractpage, gistfillitupvec,
    gistfitpage, gistgetadjusted, gistjoinvector, gistnospace, gistoptions, gistpenalty,
    gistproperty, gisttranslatecmptype, gistunion, initGISTstate, IndexAMProperty,
};

/// Install this crate's inward seams.
///
/// This crate now owns the GiST VACUUM lane (`gistvacuum.c`), so it installs the
/// AM-vtable bulk-delete / cleanup callbacks declared in
/// `backend-access-gist-am-seams` (the `ambulkdelete` / `amvacuumcleanup`
/// slots), adapting the seam shape (`callback_state: Option<u64>` handle) to the
/// owned-value bodies in [`gist_vacuum`].
///
/// The GiST core's *outward*-facing utility functions (`initGISTstate` /
/// `gistFormTuple` / page primitives) are reached by name by the sibling scan /
/// insert / build layers; the `backend-access-gist-core-seams` WAL rmgr
/// callbacks are installed by the gistxlog layer (F7) in a later stage.
pub fn init_seams() {
    backend_access_gist_am_seams::gistbulkdelete::set(
        |mcx, info, stats, callback_state| gistbulkdelete(mcx, info, stats, callback_state),
    );
    backend_access_gist_am_seams::gistvacuumcleanup::set(
        |mcx, info, stats| gistvacuumcleanup(mcx, info, stats),
    );
}
