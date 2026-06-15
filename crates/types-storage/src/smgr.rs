//! Storage-manager value types (`storage/smgr.h`, `storage/md.h`, `pg_config.h`)
//! — the boundary view of an `SMgrRelation`, the magnetic-disk per-fork segment
//! state, and the segment-geometry constants.
//!
//! C keeps the md-private per-fork fd arrays (`md_seg_fds` / `md_num_open_segs`)
//! *inside* `SMgrRelationData`. The boundary [`SMgrRelationData`] here
//! deliberately omits them — it is the shared view used by the buffer manager,
//! catalog, and AIO — and the md owner carries them alongside in
//! [`MdRelnState`].

use alloc::vec::Vec;

use types_core::primitive::{BlockNumber, InvalidBlockNumber, MAX_FORKNUM};
use types_core::BLCKSZ;

use crate::file::File;
use crate::relfilelocator::RelFileLocatorBackend;

/// `SMGR_NFORKS` — `MAX_FORKNUM + 1`, the number of relation forks. With
/// `MAX_FORKNUM == INIT_FORKNUM == 3` this is 4 (MAIN/FSM/VM/INIT).
pub const SMGR_NFORKS: usize = MAX_FORKNUM as usize + 1;

/// `SMGR_MD` (`smgr.h`) — the magnetic-disk storage-manager index in `smgrsw[]`
/// (the only registered manager).
pub const SMGR_MD: i32 = 0;

/// `RELSEG_SIZE` (`pg_config.h`) — the number of blocks per on-disk relation
/// segment file. PostgreSQL's default `--with-segsize=1` (1 GB) gives
/// `1 GiB / BLCKSZ = 1073741824 / 8192 = 131072` blocks.
pub const RELSEG_SIZE: BlockNumber = (1024 * 1024 * 1024) / BLCKSZ as BlockNumber;

/// `PG_IOV_MAX` (`port/pg_iovec.h`) — `Min(IOV_MAX, 128)`; the per-call
/// vectored-I/O block-run cap md applies inside `mdreadv` / `mdwritev`.
pub const PG_IOV_MAX: usize = 128;

// --- md.c `EXTENSION_*` behavior flags (md.c:103-111). Bit values verbatim. ---

/// `EXTENSION_FAIL` (`1 << 0`) — ereport if the segment is not present.
pub const EXTENSION_FAIL: i32 = 1 << 0;
/// `EXTENSION_RETURN_NULL` (`1 << 1`) — return NULL if the segment is missing.
pub const EXTENSION_RETURN_NULL: i32 = 1 << 1;
/// `EXTENSION_CREATE` (`1 << 2`) — create the segment if it does not exist.
pub const EXTENSION_CREATE: i32 = 1 << 2;
/// `EXTENSION_CREATE_RECOVERY` (`1 << 3`) — create the segment during recovery.
pub const EXTENSION_CREATE_RECOVERY: i32 = 1 << 3;
/// `EXTENSION_DONT_OPEN` (`1 << 5`) — only return an already-open segment.
pub const EXTENSION_DONT_OPEN: i32 = 1 << 5;

/// `SMgrRelationData` (`storage/smgr.h`) — the boundary view of an open
/// relation's storage handle. Trimmed of md's private open-segment fd arrays
/// (those live in [`MdRelnState`]).
///
/// ```c
/// typedef struct SMgrRelationData {
///     RelFileLocatorBackend smgr_rlocator;
///     BlockNumber smgr_targblock;
///     BlockNumber smgr_cached_nblocks[MAX_FORKNUM + 1];
///     int         smgr_which;
///     /* ... md-private fd arrays ... */
/// } SMgrRelationData;
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SMgrRelationData {
    /// `smgr_rlocator` — physical identity (locator + owning backend).
    pub smgr_rlocator: RelFileLocatorBackend,
    /// `smgr_targblock` — current insertion target block, or
    /// `InvalidBlockNumber`.
    pub smgr_targblock: BlockNumber,
    /// `smgr_cached_nblocks[MAX_FORKNUM + 1]` — last-known size of each fork,
    /// `InvalidBlockNumber` when not cached.
    pub smgr_cached_nblocks: [BlockNumber; SMGR_NFORKS],
    /// `smgr_which` — index into `smgrsw[]` (always `SMGR_MD`).
    pub smgr_which: i32,
}

impl SMgrRelationData {
    /// A freshly-`smgropen`'d entry: `SMGR_MD`, invalid target/cached sizes.
    pub fn new(smgr_rlocator: RelFileLocatorBackend) -> Self {
        SMgrRelationData {
            smgr_rlocator,
            smgr_targblock: InvalidBlockNumber,
            smgr_cached_nblocks: [InvalidBlockNumber; SMGR_NFORKS],
            smgr_which: SMGR_MD,
        }
    }
}

/// `_MdfdVec` (`md.c`) — one open segment of a relation fork.
///
/// ```c
/// typedef struct _MdfdVec {
///     File        mdfd_vfd;        /* fd number in fd.c's pool */
///     BlockNumber mdfd_segno;      /* segment number, from 0 */
/// } MdfdVec;
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MdfdVec {
    /// `mdfd_vfd` — the fd.c virtual file descriptor of this segment.
    pub mdfd_vfd: File,
    /// `mdfd_segno` — the segment number (0-based).
    pub mdfd_segno: BlockNumber,
}

impl Default for MdfdVec {
    fn default() -> Self {
        // C's segment vector is zero-allocated; File(0) is the never-usable
        // VFD free-list header, never a live segment fd, so a default entry is
        // always overwritten before use.
        MdfdVec {
            mdfd_vfd: File(0),
            mdfd_segno: 0,
        }
    }
}

/// The md-private per-fork open-segment state C keeps inside `SMgrRelationData`
/// (`md_seg_fds` / `md_num_open_segs`). Carried alongside the boundary
/// [`SMgrRelationData`] by the md owner's backend-local cache. These are kernel
/// fds held by one backend; never shared memory.
#[derive(Clone, Debug, Default)]
pub struct MdRelnState {
    /// `md_num_open_segs[MAX_FORKNUM + 1]` — number of open segments per fork.
    pub md_num_open_segs: [i32; SMGR_NFORKS],
    /// `md_seg_fds[MAX_FORKNUM + 1]` — open segment fd array per fork. The C
    /// `MdfdVec *` becomes an owned `Vec<MdfdVec>`; `_fdvec_resize` keeps its
    /// high-water-mark capacity so `mdtruncate` allocates no memory.
    pub md_seg_fds: [Vec<MdfdVec>; SMGR_NFORKS],
}
