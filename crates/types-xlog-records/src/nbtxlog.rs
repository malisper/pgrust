//! Btree rmgr WAL record bodies (`access/nbtxlog.h`), trimmed to the fields
//! ports consume so far.

use crate::arrays::OffsetNumbers;
use crate::bytes::{bool_at, full_xid_at, locator_at, u16_at, u32_at};
use types_core::{BlockNumber, FullTransactionId, OffsetNumber, TransactionId};
use types_storage::RelFileLocator;

/// `xl_btree_insert`: `{OffsetNumber offnum;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_btree_insert {
    pub offnum: OffsetNumber,
}

impl xl_btree_insert {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self { offnum: u16_at(rec, 0) }
    }
}

/// `xl_btree_split`: `{uint32 level; OffsetNumber firstrightoff;
/// OffsetNumber newitemoff; uint16 postingoff;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_btree_split {
    pub level: u32,
    pub firstrightoff: OffsetNumber,
    pub newitemoff: OffsetNumber,
    pub postingoff: u16,
}

impl xl_btree_split {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            level: u32_at(rec, 0),
            firstrightoff: u16_at(rec, 4),
            newitemoff: u16_at(rec, 6),
            postingoff: u16_at(rec, 8),
        }
    }
}

/// `xl_btree_dedup`: `{uint16 nintervals;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_btree_dedup {
    pub nintervals: u16,
}

impl xl_btree_dedup {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self { nintervals: u16_at(rec, 0) }
    }
}

/// `xl_btree_vacuum`: `{uint16 ndeleted; uint16 nupdated;}` — the deleted /
/// updated offset arrays and `xl_btree_update` items live in block 0's data.
#[derive(Clone, Copy, Debug)]
pub struct xl_btree_vacuum {
    pub ndeleted: u16,
    pub nupdated: u16,
}

impl xl_btree_vacuum {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            ndeleted: u16_at(rec, 0),
            nupdated: u16_at(rec, 2),
        }
    }
}

/// `xl_btree_delete`: `{TransactionId snapshotConflictHorizon;
/// uint16 ndeleted; uint16 nupdated; bool isCatalogRel;}` — the payload
/// arrays live in block 0's data, as for [`xl_btree_vacuum`].
#[derive(Clone, Copy, Debug)]
pub struct xl_btree_delete {
    pub snapshotConflictHorizon: TransactionId,
    pub ndeleted: u16,
    pub nupdated: u16,
    pub isCatalogRel: bool,
}

impl xl_btree_delete {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            snapshotConflictHorizon: u32_at(rec, 0),
            ndeleted: u16_at(rec, 4),
            nupdated: u16_at(rec, 6),
            isCatalogRel: bool_at(rec, 8),
        }
    }
}

/// `xl_btree_update`: `{uint16 ndeletedtids;}` — `ndeletedtids` posting-list
/// `uint16` offsets follow.
#[derive(Clone, Copy, Debug)]
pub struct xl_btree_update {
    pub ndeletedtids: u16,
}

/// `SizeOfBtreeUpdate` —
/// `offsetof(xl_btree_update, ndeletedtids) + sizeof(uint16)`.
pub const SIZE_OF_BTREE_UPDATE: usize = 2;

impl xl_btree_update {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self { ndeletedtids: u16_at(rec, 0) }
    }

    /// The trailing posting-list offsets (`uint16` offsets into the posting
    /// tuple's ItemPointerData array).
    pub fn ptids(rec: &[u8]) -> OffsetNumbers<'_> {
        OffsetNumbers::from_bytes(&rec[SIZE_OF_BTREE_UPDATE..])
    }
}

/// `xl_btree_mark_page_halfdead`: parent offset plus the blocks needed to
/// recreate the leaf page.
#[derive(Clone, Copy, Debug)]
pub struct xl_btree_mark_page_halfdead {
    pub poffset: OffsetNumber,
    pub leafblk: BlockNumber,
    pub leftblk: BlockNumber,
    pub rightblk: BlockNumber,
    pub topparent: BlockNumber,
}

impl xl_btree_mark_page_halfdead {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            poffset: u16_at(rec, 0),
            leafblk: u32_at(rec, 4),
            leftblk: u32_at(rec, 8),
            rightblk: u32_at(rec, 12),
            topparent: u32_at(rec, 16),
        }
    }
}

/// `xl_btree_unlink_page`: siblings/level, the deletion `safexid`
/// (8-aligned at 16), and the half-dead-leaf reconstruction blocks.
#[derive(Clone, Copy, Debug)]
pub struct xl_btree_unlink_page {
    pub leftsib: BlockNumber,
    pub rightsib: BlockNumber,
    pub level: u32,
    pub safexid: FullTransactionId,
    pub leafleftsib: BlockNumber,
    pub leafrightsib: BlockNumber,
    pub leaftopparent: BlockNumber,
}

impl xl_btree_unlink_page {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            leftsib: u32_at(rec, 0),
            rightsib: u32_at(rec, 4),
            level: u32_at(rec, 8),
            safexid: full_xid_at(rec, 16),
            leafleftsib: u32_at(rec, 24),
            leafrightsib: u32_at(rec, 28),
            leaftopparent: u32_at(rec, 32),
        }
    }
}

/// `xl_btree_newroot`: `{BlockNumber rootblk; uint32 level;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_btree_newroot {
    pub rootblk: BlockNumber,
    pub level: u32,
}

impl xl_btree_newroot {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            rootblk: u32_at(rec, 0),
            level: u32_at(rec, 4),
        }
    }
}

/// `xl_btree_reuse_page`: `{RelFileLocator locator; BlockNumber block;
/// FullTransactionId snapshotConflictHorizon /*8-aligned at 16*/;
/// bool isCatalogRel;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_btree_reuse_page {
    pub locator: RelFileLocator,
    pub block: BlockNumber,
    pub snapshotConflictHorizon: FullTransactionId,
    pub isCatalogRel: bool,
}

impl xl_btree_reuse_page {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            locator: locator_at(rec, 0),
            block: u32_at(rec, 12),
            snapshotConflictHorizon: full_xid_at(rec, 16),
            isCatalogRel: bool_at(rec, 24),
        }
    }
}

/// `xl_btree_metadata` (carried in block 0's data): trimmed to
/// `last_cleanup_num_delpages`; layout `{uint32 version /*0*/;
/// BlockNumber root /*4*/; uint32 level /*8*/; BlockNumber fastroot /*12*/;
/// uint32 fastlevel /*16*/; uint32 last_cleanup_num_delpages /*20*/;
/// bool allequalimage /*24*/;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_btree_metadata {
    pub last_cleanup_num_delpages: u32,
}

impl xl_btree_metadata {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            last_cleanup_num_delpages: u32_at(rec, 20),
        }
    }
}
