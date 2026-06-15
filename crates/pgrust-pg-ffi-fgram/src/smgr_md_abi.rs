//! Storage-manager lower-layer + bulk-write ABI vocabulary.
//!
//! Mirrors the C definitions in `src/backend/storage/smgr/md.c`,
//! `src/backend/storage/smgr/bulk_write.c`, and `src/include/storage/smgr.h`.
//!
//! This module is deliberately NOT glob-re-exported at the crate root
//! (`pub mod smgr_md_abi;` only, like the `tcop` module convention) so that the
//! md-specific `MdfdVec`/`BulkWriteState`/`f_smgr` types — which overlap in
//! purpose with the boundary-only `SMgrRelationData` in `storage.rs` — do not
//! create ambiguous-glob collisions with `crate::storage::*`. Reach them via the
//! module path: `pgrust_pg_ffi::smgr_md_abi::MdfdVec`, etc.
//!
//! These types carry the md.c "private to smgr.c and its submodules" fields
//! (`md_num_open_segs` / `md_seg_fds`) plus the `f_smgr` dispatch vtable and the
//! `BulkWriteState`/`MdfdVec` records that the magnetic-disk SM and the bulk
//! writer keep. They are placed here (not in the lower crates) because they are
//! the shared boundary vocabulary between smgr.c, md.c and bulk_write.c.

use core::ffi::c_int;

use crate::storage::{File, InvalidBlockNumber, RelFileLocatorBackend};
use crate::types::{BlockNumber, ForkNumber, XLogRecPtr};

// ---------------------------------------------------------------------------
// md.c segment-descriptor records (smgr.h SMgrRelationData private fields)
// ---------------------------------------------------------------------------

/// `_MdfdVec` (md.c:81-85): one open segment file of one fork. md.c keeps a
/// per-fork `palloc`'d array of these in the `MdCxt` memory context, with the
/// array length stored in `md_num_open_segs[forknum]`.
///
/// `repr(C)`, field-for-field with md.c so the per-fork `md_seg_fds[]` arrays
/// have the C stride. `mdfd_vfd` is an fd.c VFD pool index (`File`), NOT a raw
/// kernel fd.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MdfdVec {
    /// `mdfd_vfd` — fd number in fd.c's pool (a `File`, i.e. a VFD index).
    pub mdfd_vfd: File,
    /// `mdfd_segno` — segment number, from 0.
    pub mdfd_segno: BlockNumber,
}

impl Default for MdfdVec {
    fn default() -> Self {
        // -1 is fd.c's invalid `File`; segment 0 is the natural empty default.
        Self {
            mdfd_vfd: -1,
            mdfd_segno: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// EXTENSION_* behavior flags for mdopen & _mdfd_getseg (md.c:101-111)
// ---------------------------------------------------------------------------

/// `EXTENSION_FAIL` — ereport if segment not present.
pub const EXTENSION_FAIL: c_int = 1 << 0;
/// `EXTENSION_RETURN_NULL` — return NULL if segment not present.
pub const EXTENSION_RETURN_NULL: c_int = 1 << 1;
/// `EXTENSION_CREATE` — create new segments as needed.
pub const EXTENSION_CREATE: c_int = 1 << 2;
/// `EXTENSION_CREATE_RECOVERY` — create new segments if needed during recovery.
pub const EXTENSION_CREATE_RECOVERY: c_int = 1 << 3;
/// `EXTENSION_DONT_OPEN` — don't try to open a segment, if not already open.
/// (Note: md.c uses bit 5, leaving bit 4 unused, matching upstream exactly.)
pub const EXTENSION_DONT_OPEN: c_int = 1 << 5;

// ---------------------------------------------------------------------------
// md.c path / segment sizing constants (md.c:121-127, pg_config.h)
// ---------------------------------------------------------------------------

/// `SEGMENT_CHARS` (md.c:121) == `OIDCHARS`: max chars for a segment number.
pub const SEGMENT_CHARS: usize = crate::storage::OIDCHARS;

/// `RELSEG_SIZE` (pg_config.h): number of `BLCKSZ` blocks per segment file
/// (the 2 GB-ish OS file-size limit broken into 1 GB segments). For the
/// standard `BLCKSZ == 8192`, `RELSEG_SIZE == 131072` (= 1 GiB / 8 KiB).
pub const RELSEG_SIZE: BlockNumber = 131_072;

/// `PG_IOV_MAX` (`src/include/port/pg_iovec.h:47`): the maximum number of
/// `iovec` entries a single vectored I/O may use, defined in C as
/// `#define PG_IOV_MAX Min(IOV_MAX, 128)`. `IOV_MAX` is the kernel's per-call
/// `readv`/`writev` vector limit (at least 16 per X/Open `_XOPEN_IOV_MAX`, and
/// 1024 on Linux and macOS), so the `Min` lands on **128** on every supported
/// platform. md caps `nblocks_this_segment` by this in `mdreadv`/`mdwritev`
/// (`Min(.., lengthof(iov))`, where `iov` is the stack `struct iovec
/// iov[PG_IOV_MAX]`). This is the single shared definition both the md storage
/// manager (`backend-storage-smgr-md`) and the smgr dispatch layer
/// (`backend-storage-smgr`) reference, so they cannot disagree.
pub const PG_IOV_MAX: usize = 128;

// ---------------------------------------------------------------------------
// f_smgr dispatch vtable (smgr.c:88-126)
// ---------------------------------------------------------------------------

/// `SMgrId` selector value for the magnetic-disk SM in the `smgrsw[]` table.
/// (Re-exported from `storage.rs`'s `SMGR_MD` for callers that want it via the
/// md-abi module; the canonical constant lives in `crate::storage`.)
pub const SMGR_MD: c_int = crate::storage::SMGR_MD as c_int;

/// `NSmgr` (smgr.c:154) == `lengthof(smgrsw)`: number of registered storage
/// managers. Only the magnetic-disk SM exists today.
pub const NSMGR: usize = 1;

// ---------------------------------------------------------------------------
// bulk_write.c state (bulk_write.h: opaque BulkWriteState; bulk_write.c:51-79)
// ---------------------------------------------------------------------------

/// `MAX_PENDING_WRITES` (bulk_write.c:47) == `XLR_MAX_BLOCK_ID` (xlogrecord.h):
/// the maximum number of pages the bulk writer batches before flushing /
/// WAL-logging them in one record.
pub const MAX_PENDING_WRITES: usize = 32;

/// `PendingWrite` (bulk_write.c:51-56): one queued page write held by the bulk
/// writer until the batch is flushed.
///
/// `buf` is the `BulkWriteBuffer` (a `PGIOAlignedBlock *`); represented here as
/// an opaque pointer-sized handle since the page buffer itself lives in the
/// bulk-writer's memory context and crosses the value layer.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PendingWrite {
    /// `buf` — the queued `BulkWriteBuffer` (owned; freed after the write).
    pub buf: *mut core::ffi::c_void,
    /// `blkno` — destination block number within the fork.
    pub blkno: BlockNumber,
    /// `page_std` — whether the page uses the standard page layout (affects how
    /// it is WAL-logged via `log_newpages`).
    pub page_std: bool,
}

impl Default for PendingWrite {
    fn default() -> Self {
        Self {
            buf: core::ptr::null_mut(),
            blkno: 0,
            page_std: false,
        }
    }
}

/// `BulkWriteState` (bulk_write.c:61-79): the bulk writer's per-relation-fork
/// state. "Contents are private to bulk_write.c" (bulk_write.h:20), so this is
/// the in-crate owned record for the bulk-write port; `repr(C)` for layout
/// fidelity.
///
/// `smgr` is the target `SMgrRelation` identified here by its
/// `RelFileLocatorBackend` rather than a raw `*mut SMgrRelationData`, keeping
/// the ABI free of aliasing pointers (the smgr crate owns the relation table).
/// `memcxt` (the bulk writer's `MemoryContext`) is a backend-runtime concern
/// carried opaquely.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BulkWriteState {
    /// `smgr` — target relation (the `SMgrRelation`'s physical identifier).
    pub smgr: RelFileLocatorBackend,
    /// `forknum` — target fork.
    pub forknum: ForkNumber,
    /// `use_wal` — whether to WAL-log the pages.
    pub use_wal: bool,
    /// `npending` — number of queued writes in `pending_writes`.
    pub npending: c_int,
    /// `pending_writes` — the batch of queued page writes.
    pub pending_writes: [PendingWrite; MAX_PENDING_WRITES],
    /// `relsize` — current size of the relation fork (in blocks).
    pub relsize: BlockNumber,
    /// `start_RedoRecPtr` — the `RedoRecPtr` at the time the bulk op started;
    /// compared at finish to detect a concurrent checkpoint.
    pub start_RedoRecPtr: XLogRecPtr,
    /// `memcxt` — the memory context used to allocate the page buffers; opaque
    /// pointer-sized handle (backend-runtime concern).
    pub memcxt: *mut core::ffi::c_void,
}

impl Default for BulkWriteState {
    fn default() -> Self {
        Self {
            smgr: RelFileLocatorBackend {
                locator: crate::wal::RelFileLocator::new(0, 0, 0),
                backend: crate::types::INVALID_PROC_NUMBER,
            },
            forknum: 0,
            use_wal: false,
            npending: 0,
            pending_writes: [PendingWrite::default(); MAX_PENDING_WRITES],
            relsize: InvalidBlockNumber,
            start_RedoRecPtr: 0,
            memcxt: core::ptr::null_mut(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn mdfdvec_layout_matches_c() {
        // _MdfdVec { File mdfd_vfd; BlockNumber mdfd_segno; } -> 8 bytes.
        assert_eq!(size_of::<MdfdVec>(), 8);
        assert_eq!(align_of::<MdfdVec>(), 4);
        assert_eq!(offset_of!(MdfdVec, mdfd_vfd), 0);
        assert_eq!(offset_of!(MdfdVec, mdfd_segno), 4);
    }

    #[test]
    fn extension_flag_values_match_md_c() {
        assert_eq!(EXTENSION_FAIL, 1);
        assert_eq!(EXTENSION_RETURN_NULL, 2);
        assert_eq!(EXTENSION_CREATE, 4);
        assert_eq!(EXTENSION_CREATE_RECOVERY, 8);
        assert_eq!(EXTENSION_DONT_OPEN, 32);
    }

    #[test]
    fn segment_constants_match() {
        assert_eq!(SEGMENT_CHARS, 10);
        assert_eq!(RELSEG_SIZE, 131_072);
        // PG_IOV_MAX == Min(IOV_MAX, 128) == 128 on every supported platform.
        assert_eq!(PG_IOV_MAX, 128);
        assert_eq!(MAX_PENDING_WRITES, 32);
        assert_eq!(NSMGR, 1);
        assert_eq!(crate::storage::SMGR_NFORKS, 4);
    }
}
