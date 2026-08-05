//! nbtxlog.h record images: byte layouts match the C structs exactly
//! (SizeOfBtreeInsert 2, SizeOfBtreeSplit 10, SizeOfBtreeNewroot 8,
//! sizeof(xl_btree_metadata) 28 incl. trailing pad, SizeOfBtreeVacuum 4,
//! SizeOfBtreeMarkPageHalfDead 20 incl. pad @2, SizeOfBtreeUnlinkPage 36
//! incl. pad @12, SizeOfBtreeReusePage 25).

use ::types_core::xact::FullTransactionId;
use ::types_core::{BlockNumber, OffsetNumber};
use ::types_nbtree::BTMetaPageData;
use ::types_storage::RelFileLocator;

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

pub(crate) fn xl_btree_vacuum(ndeleted: u16, nupdated: u16) -> [u8; 4] {
    let mut b = [0u8; 4];
    b[0..2].copy_from_slice(&ndeleted.to_ne_bytes());
    b[2..4].copy_from_slice(&nupdated.to_ne_bytes());
    b
}

pub(crate) fn xl_btree_mark_page_halfdead(
    poffset: OffsetNumber,
    leafblk: BlockNumber,
    leftblk: BlockNumber,
    rightblk: BlockNumber,
    topparent: BlockNumber,
) -> [u8; 20] {
    let mut b = [0u8; 20];
    b[0..2].copy_from_slice(&poffset.to_ne_bytes());
    b[4..8].copy_from_slice(&leafblk.to_ne_bytes());
    b[8..12].copy_from_slice(&leftblk.to_ne_bytes());
    b[12..16].copy_from_slice(&rightblk.to_ne_bytes());
    b[16..20].copy_from_slice(&topparent.to_ne_bytes());
    b
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn xl_btree_unlink_page(
    leftsib: BlockNumber,
    rightsib: BlockNumber,
    level: u32,
    safexid: FullTransactionId,
    leafleftsib: BlockNumber,
    leafrightsib: BlockNumber,
    leaftopparent: BlockNumber,
) -> [u8; 36] {
    let mut b = [0u8; 36];
    b[0..4].copy_from_slice(&leftsib.to_ne_bytes());
    b[4..8].copy_from_slice(&rightsib.to_ne_bytes());
    b[8..12].copy_from_slice(&level.to_ne_bytes());
    b[16..24].copy_from_slice(&safexid.value.to_ne_bytes());
    b[24..28].copy_from_slice(&leafleftsib.to_ne_bytes());
    b[28..32].copy_from_slice(&leafrightsib.to_ne_bytes());
    b[32..36].copy_from_slice(&leaftopparent.to_ne_bytes());
    b
}

pub(crate) fn xl_btree_reuse_page(
    locator: RelFileLocator,
    block: BlockNumber,
    safexid: FullTransactionId,
) -> [u8; 25] {
    let mut b = [0u8; 25];
    b[0..4].copy_from_slice(&locator.spcOid.to_ne_bytes());
    b[4..8].copy_from_slice(&locator.dbOid.to_ne_bytes());
    b[8..12].copy_from_slice(&locator.relNumber.to_ne_bytes());
    b[12..16].copy_from_slice(&block.to_ne_bytes());
    b[16..24].copy_from_slice(&safexid.value.to_ne_bytes());
    // isCatalogRel: RelationIsAccessibleInLogicalDecoding const-false.
    b[24] = 0;
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

// SizeOfBtreeDelete = offsetof(isCatalogRel) + sizeof(bool) = 9.
pub(crate) fn xl_btree_delete(
    snapshot_conflict_horizon: ::types_core::TransactionId,
    ndeleted: u16,
    nupdated: u16,
    is_catalog_rel: bool,
) -> [u8; 9] {
    let mut b = [0u8; 9];
    b[0..4].copy_from_slice(&snapshot_conflict_horizon.to_ne_bytes());
    b[4..6].copy_from_slice(&ndeleted.to_ne_bytes());
    b[6..8].copy_from_slice(&nupdated.to_ne_bytes());
    b[8] = is_catalog_rel as u8;
    b
}
