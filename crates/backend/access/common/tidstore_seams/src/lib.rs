//! Seams for the dead-TID radix-tree store (`src/backend/access/common/tidstore.c`,
//! `access/tidstore.h`).
//!
//! These are **outward** seams that VACUUM (and the lazy heap-vacuum driver)
//! reaches into the TID store. They were previously mis-homed in
//! `backend-access-heap-vacuumlazy-seams`; the true owner is the separate
//! c2rust unit `backend-access-common-tidstore`, which installs them from its
//! `init_seams()`. Moved here so the seam crate's stem matches its owner.

#![allow(non_snake_case)]

use alloc::vec::Vec;

extern crate alloc;

use types_core::{BlockNumber, OffsetNumber};
use ::types_error::PgResult;
use ::types_vacuum::vacuumlazy::{ReapBlockInfo, TidStore, TidStoreIterHandle};

// =======================================================================
// access/tidstore.h — the dead-TID radix-tree store.
// =======================================================================

seam_core::seam!(
    /// `TidStoreCreateLocal(max_bytes, insert_only)`.
    pub fn tidstore_create_local(max_bytes: usize, insert_only: bool) -> PgResult<TidStore>
);
seam_core::seam!(
    /// `TidStoreDestroy(ts)`.
    pub fn tidstore_destroy(ts: TidStore) -> PgResult<()>
);
seam_core::seam!(
    /// `TidStoreSetBlockOffsets(ts, blkno, offsets, num_offsets)`.
    pub fn tidstore_set_block_offsets(
        ts: TidStore,
        blkno: BlockNumber,
        offsets: Vec<OffsetNumber>,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `TidStoreMemoryUsage(ts)` (bytes).
    pub fn tidstore_memory_usage(ts: TidStore) -> PgResult<usize>
);
seam_core::seam!(
    /// `TidStoreBeginIterate(ts)`.
    pub fn tidstore_begin_iterate(ts: TidStore) -> PgResult<TidStoreIterHandle>
);
seam_core::seam!(
    /// `TidStoreIterateNext(iter)` — `None` at end.
    pub fn tidstore_iterate_next(iter: TidStoreIterHandle) -> PgResult<Option<ReapBlockInfo>>
);
seam_core::seam!(
    /// `TidStoreEndIterate(iter)`.
    pub fn tidstore_end_iterate(iter: TidStoreIterHandle) -> PgResult<()>
);
seam_core::seam!(
    /// `TidStoreIsMember(ts, tid)` — membership probe used by VACUUM's
    /// `vac_tid_reaped` index-bulkdelete callback. Outward call into
    /// `access/tidstore.c`; panics until that owner lands.
    pub fn tidstore_is_member(
        ts: TidStore,
        tid: types_tuple::heaptuple::ItemPointerData,
    ) -> PgResult<bool>
);
