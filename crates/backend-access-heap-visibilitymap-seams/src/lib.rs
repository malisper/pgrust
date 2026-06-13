//! Seam declarations for the `backend-access-heap-visibilitymap` unit
//! (`access/heap/visibilitymap.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

/// `VISIBILITYMAP_ALL_VISIBLE` (visibilitymapdefs.h) — the VM status bit set
/// when every tuple on the heap page is known visible to all transactions.
pub const VISIBILITYMAP_ALL_VISIBLE: u8 = 0x01;
/// `VISIBILITYMAP_ALL_FROZEN` (visibilitymapdefs.h).
pub const VISIBILITYMAP_ALL_FROZEN: u8 = 0x02;

seam_core::seam!(
    /// `visibilitymap_get_status(rel, heapBlk, &vmbuf)` (visibilitymap.c):
    /// read the visibility-map status bits for a heap block, pinning (and
    /// re-pinning across calls) the VM buffer recorded in `vmbuf`. Returns the
    /// status byte (`VISIBILITYMAP_ALL_VISIBLE`/`_ALL_FROZEN` bits) and the
    /// possibly-updated VM buffer. Reading the VM does not lock the buffer.
    /// Fallible on the smgr `ereport(ERROR)`s.
    pub fn visibilitymap_get_status<'mcx>(
        rel: types_rel::Relation<'mcx>,
        heap_blk: types_core::primitive::BlockNumber,
        vmbuf: types_storage::Buffer,
    ) -> types_error::PgResult<(u8, types_storage::Buffer)>
);
