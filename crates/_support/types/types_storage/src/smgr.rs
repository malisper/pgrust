use alloc::vec::Vec;

use ::types_core::primitive::{BlockNumber, InvalidBlockNumber, MAX_FORKNUM};
use ::types_core::BLCKSZ;

use crate::file::File;
use crate::relfilelocator::RelFileLocatorBackend;

pub const SMGR_NFORKS: usize = MAX_FORKNUM as usize + 1;

pub const SMGR_MD: i32 = 0;

// --with-segsize=1 default: 1 GiB / BLCKSZ.
pub const RELSEG_SIZE: BlockNumber = (1024 * 1024 * 1024) / BLCKSZ as BlockNumber;

pub const PG_IOV_MAX: usize = 128;

pub const EXTENSION_FAIL: i32 = 1 << 0;
pub const EXTENSION_RETURN_NULL: i32 = 1 << 1;
pub const EXTENSION_CREATE: i32 = 1 << 2;
pub const EXTENSION_CREATE_RECOVERY: i32 = 1 << 3;
pub const EXTENSION_DONT_OPEN: i32 = 1 << 5;

// C's `SMgrRelation` pointer as slab idx+gen: slot reuse bumps `gen` (stale handle = loud); pinned entries never move or die (smgr.c pin contract), so rd_smgr may cache this.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SmgrHandle {
    pub idx: u32,
    pub gen: core::num::NonZeroU32,
}

const _: () = assert!(core::mem::size_of::<Option<SmgrHandle>>() == 8);

// The boundary view of C's SMgrRelationData: md's private open-segment fd
// arrays (md_seg_fds / md_num_open_segs) live in MdRelnState beside it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SMgrRelationData {
    pub smgr_rlocator: RelFileLocatorBackend,
    pub smgr_targblock: BlockNumber,
    pub smgr_cached_nblocks: [BlockNumber; SMGR_NFORKS],
    pub smgr_which: i32,
}

impl SMgrRelationData {
    pub fn new(smgr_rlocator: RelFileLocatorBackend) -> Self {
        SMgrRelationData {
            smgr_rlocator,
            smgr_targblock: InvalidBlockNumber,
            smgr_cached_nblocks: [InvalidBlockNumber; SMGR_NFORKS],
            smgr_which: SMGR_MD,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MdfdVec {
    pub mdfd_vfd: File,
    pub mdfd_segno: BlockNumber,
}

impl Default for MdfdVec {
    // C's segment vector is zero-allocated; File(0) is the never-usable VFD
    // free-list header, always overwritten before use.
    fn default() -> Self {
        MdfdVec {
            mdfd_vfd: File(0),
            mdfd_segno: 0,
        }
    }
}

// Backend-local kernel fds; _fdvec_resize keeps high-water-mark capacity. std Vec justified: backend-lifetime owner state (C's MdCxt), no spill reads it.
#[derive(Clone, Debug, Default)]
pub struct MdRelnState {
    pub md_num_open_segs: [i32; SMGR_NFORKS],
    pub md_seg_fds: [Vec<MdfdVec>; SMGR_NFORKS],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_geometry_matches_pg_config() {
        assert_eq!(SMGR_NFORKS, 4);
        assert_eq!(RELSEG_SIZE, 131072);
    }
}
