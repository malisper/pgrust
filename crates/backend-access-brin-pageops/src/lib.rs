//! `backend-access-brin-pageops` — owned-tree Rust port of the BRIN
//! engine's page/range-map layer (PostgreSQL 18.3):
//!
//!   * `src/backend/access/brin/brin_pageops.c` — page-handling routines.
//!   * `src/backend/access/brin/brin_revmap.c` — the reverse range map.
//!
//! These two C files are mutually recursive (`revmap_physical_extend` calls
//! `brin_start_evacuating_page` / `brin_evacuate_page`, which call
//! `brin_doupdate`, which calls `brinRevmapExtend` /
//! `brinLockRevmapPageForUpdate`), so they cannot live in two separate crates
//! (Rust forbids crate cycles). This crate therefore folds in both files — its
//! catalog lineage matches the existing per-file BRIN crates
//! (`backend-access-brin-tuple`, `backend-access-brin-xlog`); `brin_revmap.c`
//! is its mutually-recursive partner, not a separate compilation unit here.
//!
//! The buffer cache, WAL, FSM, and lmgr cross to their owners through the real
//! repo seam crates — exactly the substrate the btree/hash index towers use:
//!
//!   * bufmgr (`with_buffer_page` / `lock_buffer` / `mark_buffer_dirty` /
//!     `mark_buffer_dirty_hint` / `unlock_release_buffer` / `release_buffer` /
//!     `buffer_get_block_number` / `read_buffer` / `extend_buffered_rel`),
//!   * relcache (`relation_needs_wal` / `relation_is_local` /
//!     `relation_get_number_of_blocks`),
//!   * freespace (`get_page_with_free_space` / `record_page_with_free_space` /
//!     `record_and_get_page_with_free_space` / `free_space_map_vacuum_range`),
//!   * lmgr (`lock_relation_for_extension`),
//!   * xloginsert (`xlog_begin_insert` / `xlog_register_*` /
//!     `xlog_insert_record` / `log_newpage_buffer`).
//!
//! The BRIN-specific page-byte primitives (`brin_page_init` /
//! `brin_metapage_init`, the `BrinPageType` / `BrinPageFlags` /
//! `BrinMetaPageData` accessors from `brin_page.h`, the `RevmapContents`
//! item-pointer arithmetic from `brin_revmap.c`, and the `xl_brin_*` record
//! encoders) are grounded in-crate against the `BLCKSZ` page bytes, identical
//! to the transcription already in `backend-access-brin-xlog`; the redo crate
//! moves to depend on these once it is re-pointed.
//!
//! This crate owns no inward seams — it is consumed by `brin.c` (the BRIN
//! index AM, not yet ported) — so it has no `init_seams`/seams-init line.
//!
//! No raw pointers, no `extern "C"`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
// `PgError` is large, so the un-boxed `PgResult` `Err` is large; this is the
// project-wide error contract these ports must match.
#![allow(clippy::result_large_err)]

extern crate alloc;

mod brin_internal;
mod brin_page;
mod brin_pageops;
mod brin_revmap;
mod wal;

/// `REGBUF_STANDARD` (xloginsert.h) — page follows standard layout.
pub(crate) use types_wal::xloginsert::REGBUF_STANDARD;
/// `REGBUF_WILL_INIT` (xloginsert.h) — page will be re-initialized.
pub(crate) use types_wal::xloginsert::REGBUF_WILL_INIT;

pub use brin_internal::BrinRevmap;
pub use brin_page::{
    brin_metapage_init, brin_page_init, BRIN_EVACUATE_PAGE, PAGETYPE_META, PAGETYPE_REGULAR,
    PAGETYPE_REVMAP,
};
pub use brin_pageops::{
    brin_can_do_samepage_update, brin_doinsert, brin_doupdate, brin_evacuate_page,
    brin_page_cleanup, brin_start_evacuating_page,
};
pub use brin_revmap::{
    brinGetTupleForHeapBlock, brinLockRevmapPageForUpdate, brinRevmapDesummarizeRange,
    brinRevmapExtend, brinRevmapInitialize, brinRevmapTerminate, brinSetHeapBlockItemptr,
    FoundTuple,
};
