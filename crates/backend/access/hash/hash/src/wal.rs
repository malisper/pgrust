//! hash_xlog.h record images: byte layouts match the C structs exactly
//! (SizeOfHashInsert 2, SizeOfHashAddOvflPage 3, SizeOfHashSplitAllocPage 9,
//! SizeOfHashSplitComplete 4, SizeOfHashMovePageContents 3,
//! SizeOfHashSqueezePage 12, SizeOfHashDelete 2, SizeOfHashUpdateMetaPage 8,
//! SizeOfHashInitMetaPage 14, SizeOfHashInitBitmapPage 2,
//! SizeOfHashVacuumOnePage 8).

use ::types_core::{BlockNumber, OffsetNumber, RegProcedure, TransactionId};

pub(crate) fn xl_hash_insert(offnum: OffsetNumber) -> [u8; 2] {
    offnum.to_ne_bytes()
}

pub(crate) fn xl_hash_add_ovfl_page(bmsize: u16, bmpage_found: bool) -> [u8; 3] {
    let mut b = [0u8; 3];
    b[0..2].copy_from_slice(&bmsize.to_ne_bytes());
    b[2] = bmpage_found as u8;
    b
}

pub(crate) fn xl_hash_split_allocate_page(
    new_bucket: u32,
    old_bucket_flag: u16,
    new_bucket_flag: u16,
    flags: u8,
) -> [u8; 9] {
    let mut b = [0u8; 9];
    b[0..4].copy_from_slice(&new_bucket.to_ne_bytes());
    b[4..6].copy_from_slice(&old_bucket_flag.to_ne_bytes());
    b[6..8].copy_from_slice(&new_bucket_flag.to_ne_bytes());
    b[8] = flags;
    b
}

pub(crate) fn xl_hash_split_complete(old_bucket_flag: u16, new_bucket_flag: u16) -> [u8; 4] {
    let mut b = [0u8; 4];
    b[0..2].copy_from_slice(&old_bucket_flag.to_ne_bytes());
    b[2..4].copy_from_slice(&new_bucket_flag.to_ne_bytes());
    b
}

pub(crate) fn xl_hash_move_page_contents(ntups: u16, is_prim_bucket_same_wrt: bool) -> [u8; 3] {
    let mut b = [0u8; 3];
    b[0..2].copy_from_slice(&ntups.to_ne_bytes());
    b[2] = is_prim_bucket_same_wrt as u8;
    b
}

pub(crate) fn xl_hash_squeeze_page(
    prevblkno: BlockNumber,
    nextblkno: BlockNumber,
    ntups: u16,
    is_prim_bucket_same_wrt: bool,
    is_prev_bucket_same_wrt: bool,
) -> [u8; 12] {
    let mut b = [0u8; 12];
    b[0..4].copy_from_slice(&prevblkno.to_ne_bytes());
    b[4..8].copy_from_slice(&nextblkno.to_ne_bytes());
    b[8..10].copy_from_slice(&ntups.to_ne_bytes());
    b[10] = is_prim_bucket_same_wrt as u8;
    b[11] = is_prev_bucket_same_wrt as u8;
    b
}

pub(crate) fn xl_hash_delete(clear_dead_marking: bool, is_primary_bucket_page: bool) -> [u8; 2] {
    [clear_dead_marking as u8, is_primary_bucket_page as u8]
}

pub(crate) fn xl_hash_update_meta_page(ntuples: f64) -> [u8; 8] {
    ntuples.to_ne_bytes()
}

pub(crate) fn xl_hash_init_meta_page(
    num_tuples: f64,
    procid: RegProcedure,
    ffactor: u16,
) -> [u8; 14] {
    let mut b = [0u8; 14];
    b[0..8].copy_from_slice(&num_tuples.to_ne_bytes());
    b[8..12].copy_from_slice(&procid.to_ne_bytes());
    b[12..14].copy_from_slice(&ffactor.to_ne_bytes());
    b
}

pub(crate) fn xl_hash_init_bitmap_page(bmsize: u16) -> [u8; 2] {
    bmsize.to_ne_bytes()
}

pub(crate) fn xl_hash_vacuum_one_page(
    snapshot_conflict_horizon: TransactionId,
    ntuples: u16,
    is_catalog_rel: bool,
) -> [u8; 8] {
    let mut b = [0u8; 8];
    b[0..4].copy_from_slice(&snapshot_conflict_horizon.to_ne_bytes());
    b[4..6].copy_from_slice(&ntuples.to_ne_bytes());
    b[6] = is_catalog_rel as u8;
    b
}
