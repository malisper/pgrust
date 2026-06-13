//! Hash rmgr WAL record bodies (`access/hash_xlog.h`), trimmed to the fields
//! ports consume so far.

use crate::bytes::{bool_at, f64_at, u16_at, u32_at, u8_at};
use types_core::{BlockNumber, OffsetNumber, RegProcedure, TransactionId};

/// `xl_hash_init_meta_page`: `{double num_tuples; RegProcedure procid;
/// uint16 ffactor;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_hash_init_meta_page {
    pub num_tuples: f64,
    pub procid: RegProcedure,
    pub ffactor: u16,
}

impl xl_hash_init_meta_page {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            num_tuples: f64_at(rec, 0),
            procid: u32_at(rec, 8),
            ffactor: u16_at(rec, 12),
        }
    }
}

/// `xl_hash_init_bitmap_page`: `{uint16 bmsize;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_hash_init_bitmap_page {
    pub bmsize: u16,
}

impl xl_hash_init_bitmap_page {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self { bmsize: u16_at(rec, 0) }
    }
}

/// `xl_hash_insert`: `{OffsetNumber offnum;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_hash_insert {
    pub offnum: OffsetNumber,
}

impl xl_hash_insert {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self { offnum: u16_at(rec, 0) }
    }
}

/// `xl_hash_add_ovfl_page`: `{uint16 bmsize; bool bmpage_found;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_hash_add_ovfl_page {
    pub bmsize: u16,
    pub bmpage_found: bool,
}

impl xl_hash_add_ovfl_page {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            bmsize: u16_at(rec, 0),
            bmpage_found: bool_at(rec, 2),
        }
    }
}

/// `xl_hash_split_allocate_page`: `{uint32 new_bucket;
/// uint16 old_bucket_flag; uint16 new_bucket_flag; uint8 flags;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_hash_split_allocate_page {
    pub new_bucket: u32,
    pub old_bucket_flag: u16,
    pub new_bucket_flag: u16,
    pub flags: u8,
}

impl xl_hash_split_allocate_page {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            new_bucket: u32_at(rec, 0),
            old_bucket_flag: u16_at(rec, 4),
            new_bucket_flag: u16_at(rec, 6),
            flags: u8_at(rec, 8),
        }
    }
}

/// `xl_hash_split_complete`: `{uint16 old_bucket_flag;
/// uint16 new_bucket_flag;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_hash_split_complete {
    pub old_bucket_flag: u16,
    pub new_bucket_flag: u16,
}

impl xl_hash_split_complete {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            old_bucket_flag: u16_at(rec, 0),
            new_bucket_flag: u16_at(rec, 2),
        }
    }
}

/// `xl_hash_move_page_contents`: `{uint16 ntups;
/// bool is_prim_bucket_same_wrt;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_hash_move_page_contents {
    pub ntups: u16,
    pub is_prim_bucket_same_wrt: bool,
}

impl xl_hash_move_page_contents {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            ntups: u16_at(rec, 0),
            is_prim_bucket_same_wrt: bool_at(rec, 2),
        }
    }
}

/// `xl_hash_squeeze_page`: trimmed of the trailing
/// `is_prev_bucket_same_wrt` flag.
#[derive(Clone, Copy, Debug)]
pub struct xl_hash_squeeze_page {
    pub prevblkno: BlockNumber,
    pub nextblkno: BlockNumber,
    pub ntups: u16,
    pub is_prim_bucket_same_wrt: bool,
}

impl xl_hash_squeeze_page {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            prevblkno: u32_at(rec, 0),
            nextblkno: u32_at(rec, 4),
            ntups: u16_at(rec, 8),
            is_prim_bucket_same_wrt: bool_at(rec, 10),
        }
    }
}

/// `xl_hash_delete`: `{bool clear_dead_marking; bool is_primary_bucket_page;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_hash_delete {
    pub clear_dead_marking: bool,
    pub is_primary_bucket_page: bool,
}

impl xl_hash_delete {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            clear_dead_marking: bool_at(rec, 0),
            is_primary_bucket_page: bool_at(rec, 1),
        }
    }
}

/// `xl_hash_update_meta_page`: `{double ntuples;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_hash_update_meta_page {
    pub ntuples: f64,
}

impl xl_hash_update_meta_page {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self { ntuples: f64_at(rec, 0) }
    }
}

/// `xl_hash_vacuum_one_page`: `{TransactionId snapshotConflictHorizon;
/// uint16 ntuples; bool isCatalogRel; OffsetNumber offsets[];}` — trimmed of
/// the trailing offsets.
#[derive(Clone, Copy, Debug)]
pub struct xl_hash_vacuum_one_page {
    pub snapshotConflictHorizon: TransactionId,
    pub ntuples: u16,
    pub isCatalogRel: bool,
}

impl xl_hash_vacuum_one_page {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            snapshotConflictHorizon: u32_at(rec, 0),
            ntuples: u16_at(rec, 4),
            isCatalogRel: bool_at(rec, 6),
        }
    }
}
