use ::types_core::{uint16, BlockNumber, FullTransactionId, OffsetNumber, TransactionId};
use ::types_storage::RelFileLocator;

pub const XLOG_BTREE_INSERT_LEAF: u8 = 0x00;
pub const XLOG_BTREE_INSERT_UPPER: u8 = 0x10;
pub const XLOG_BTREE_INSERT_META: u8 = 0x20;
pub const XLOG_BTREE_SPLIT_L: u8 = 0x30;
pub const XLOG_BTREE_SPLIT_R: u8 = 0x40;
pub const XLOG_BTREE_INSERT_POST: u8 = 0x50;
pub const XLOG_BTREE_DEDUP: u8 = 0x60;
pub const XLOG_BTREE_DELETE: u8 = 0x70;
pub const XLOG_BTREE_UNLINK_PAGE: u8 = 0x80;
pub const XLOG_BTREE_UNLINK_PAGE_META: u8 = 0x90;
pub const XLOG_BTREE_NEWROOT: u8 = 0xA0;
pub const XLOG_BTREE_MARK_PAGE_HALFDEAD: u8 = 0xB0;
pub const XLOG_BTREE_VACUUM: u8 = 0xC0;
pub const XLOG_BTREE_REUSE_PAGE: u8 = 0xD0;
pub const XLOG_BTREE_META_CLEANUP: u8 = 0xE0;

// nbtxlog.h record vocabulary. The redo path decodes these field-by-field out
// of possibly unaligned WAL buffers, so they carry no #[repr(C)] ABI contract.

#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_metadata {
    pub version: u32,
    pub root: BlockNumber,
    pub level: u32,
    pub fastroot: BlockNumber,
    pub fastlevel: u32,
    pub last_cleanup_num_delpages: u32,
    pub allequalimage: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_insert {
    pub offnum: OffsetNumber,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_split {
    pub level: u32,
    pub firstrightoff: OffsetNumber,
    pub newitemoff: OffsetNumber,
    pub postingoff: uint16,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_dedup {
    pub nintervals: uint16,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_reuse_page {
    pub locator: RelFileLocator,
    pub block: BlockNumber,
    pub snapshotConflictHorizon: FullTransactionId,
    pub isCatalogRel: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_vacuum {
    pub ndeleted: uint16,
    pub nupdated: uint16,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_delete {
    pub snapshotConflictHorizon: TransactionId,
    pub ndeleted: uint16,
    pub nupdated: uint16,
    pub isCatalogRel: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_mark_page_halfdead {
    pub poffset: OffsetNumber,
    pub leafblk: BlockNumber,
    pub leftblk: BlockNumber,
    pub rightblk: BlockNumber,
    pub topparent: BlockNumber,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_unlink_page {
    pub leftsib: BlockNumber,
    pub rightsib: BlockNumber,
    pub level: u32,
    pub safexid: FullTransactionId,
    pub leafleftsib: BlockNumber,
    pub leafrightsib: BlockNumber,
    pub leaftopparent: BlockNumber,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct xl_btree_newroot {
    pub rootblk: BlockNumber,
    pub level: u32,
}

pub const SizeOfBtreeDedup: usize = 2;

pub const SizeOfBtreeUpdate: usize = 2;

#[cfg(test)]
mod xlog_tests {
    use super::*;

    #[test]
    fn opcodes_match_nbtxlog_h() {
        assert_eq!(XLOG_BTREE_INSERT_LEAF, 0x00);
        assert_eq!(XLOG_BTREE_INSERT_UPPER, 0x10);
        assert_eq!(XLOG_BTREE_INSERT_META, 0x20);
        assert_eq!(XLOG_BTREE_SPLIT_L, 0x30);
        assert_eq!(XLOG_BTREE_SPLIT_R, 0x40);
        assert_eq!(XLOG_BTREE_INSERT_POST, 0x50);
        assert_eq!(XLOG_BTREE_DEDUP, 0x60);
        assert_eq!(XLOG_BTREE_DELETE, 0x70);
        assert_eq!(XLOG_BTREE_UNLINK_PAGE, 0x80);
        assert_eq!(XLOG_BTREE_UNLINK_PAGE_META, 0x90);
        assert_eq!(XLOG_BTREE_NEWROOT, 0xA0);
        assert_eq!(XLOG_BTREE_MARK_PAGE_HALFDEAD, 0xB0);
        assert_eq!(XLOG_BTREE_VACUUM, 0xC0);
        assert_eq!(XLOG_BTREE_REUSE_PAGE, 0xD0);
        assert_eq!(XLOG_BTREE_META_CLEANUP, 0xE0);
        assert_eq!(SizeOfBtreeDedup, 2);
        assert_eq!(SizeOfBtreeUpdate, 2);
    }
}
