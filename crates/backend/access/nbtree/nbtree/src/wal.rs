//! nbtxlog.h record images: byte layouts match the C structs exactly
//! (SizeOfBtreeInsert 2, SizeOfBtreeSplit 10, SizeOfBtreeNewroot 8,
//! sizeof(xl_btree_metadata) 28 incl. trailing pad).

use ::types_core::{BlockNumber, OffsetNumber};
use ::types_nbtree::BTMetaPageData;

pub(crate) fn xl_btree_insert(offnum: OffsetNumber) -> [u8; 2] {
    offnum.to_ne_bytes()
}

pub(crate) fn xl_btree_split(
    level: u32,
    firstrightoff: OffsetNumber,
    newitemoff: OffsetNumber,
    postingoff: u16,
) -> [u8; 10] {
    let mut b = [0u8; 10];
    b[0..4].copy_from_slice(&level.to_ne_bytes());
    b[4..6].copy_from_slice(&firstrightoff.to_ne_bytes());
    b[6..8].copy_from_slice(&newitemoff.to_ne_bytes());
    b[8..10].copy_from_slice(&postingoff.to_ne_bytes());
    b
}

pub(crate) fn xl_btree_newroot(rootblk: BlockNumber, level: u32) -> [u8; 8] {
    let mut b = [0u8; 8];
    b[0..4].copy_from_slice(&rootblk.to_ne_bytes());
    b[4..8].copy_from_slice(&level.to_ne_bytes());
    b
}

pub(crate) fn xl_btree_metadata(metad: &BTMetaPageData) -> [u8; 28] {
    let mut b = [0u8; 28];
    b[0..4].copy_from_slice(&metad.btm_version.to_ne_bytes());
    b[4..8].copy_from_slice(&metad.btm_root.to_ne_bytes());
    b[8..12].copy_from_slice(&metad.btm_level.to_ne_bytes());
    b[12..16].copy_from_slice(&metad.btm_fastroot.to_ne_bytes());
    b[16..20].copy_from_slice(&metad.btm_fastlevel.to_ne_bytes());
    b[20..24].copy_from_slice(&metad.btm_last_cleanup_num_delpages.to_ne_bytes());
    b[24] = metad.btm_allequalimage as u8;
    b
}

pub(crate) fn xl_btree_dedup(nintervals: u16) -> [u8; 2] {
    nintervals.to_ne_bytes()
}
