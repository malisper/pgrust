use ::types_core::{
    uint16, BlockNumber, FullTransactionId, OffsetNumber, Size, BLCKSZ, INDEX_MAX_KEYS,
};
use ::types_scan::scankey::{BTMaxStrategyNumber, StrategyNumber};
use ::types_storage::buf::{BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_SHARE};
use ::types_storage::bufpage::{ItemIdData, SizeOfPageHeaderData};
use ::types_tuple::itemptr::ItemPointerData;

pub type BTCycleId = uint16;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BTPageOpaqueData {
    pub btpo_prev: BlockNumber,
    pub btpo_next: BlockNumber,
    pub btpo_level: u32,
    pub btpo_flags: uint16,
    pub btpo_cycleid: BTCycleId,
}

const _: () = assert!(core::mem::size_of::<BTPageOpaqueData>() == 16);
const _: () = assert!(core::mem::offset_of!(BTPageOpaqueData, btpo_prev) == 0);
const _: () = assert!(core::mem::offset_of!(BTPageOpaqueData, btpo_next) == 4);
const _: () = assert!(core::mem::offset_of!(BTPageOpaqueData, btpo_level) == 8);
const _: () = assert!(core::mem::offset_of!(BTPageOpaqueData, btpo_flags) == 12);
const _: () = assert!(core::mem::offset_of!(BTPageOpaqueData, btpo_cycleid) == 14);

pub const BTP_LEAF: uint16 = 1 << 0;
pub const BTP_ROOT: uint16 = 1 << 1;
pub const BTP_DELETED: uint16 = 1 << 2;
pub const BTP_META: uint16 = 1 << 3;
pub const BTP_HALF_DEAD: uint16 = 1 << 4;
pub const BTP_SPLIT_END: uint16 = 1 << 5;
pub const BTP_HAS_GARBAGE: uint16 = 1 << 6;
pub const BTP_INCOMPLETE_SPLIT: uint16 = 1 << 7;
pub const BTP_HAS_FULLXID: uint16 = 1 << 8;

pub const MAX_BT_CYCLE_ID: BTCycleId = 0xFF7F;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct BTMetaPageData {
    pub btm_magic: u32,
    pub btm_version: u32,
    pub btm_root: BlockNumber,
    pub btm_level: u32,
    pub btm_fastroot: BlockNumber,
    pub btm_fastlevel: u32,
    // Remaining fields only valid when btm_version >= BTREE_NOVAC_VERSION.
    pub btm_last_cleanup_num_delpages: u32,
    pub btm_last_cleanup_num_heap_tuples: f64,
    pub btm_allequalimage: bool,
}

impl BTMetaPageData {
    // On-page image with zeroed padding (offsets 28..32, 41..48): C stores
    // fields into a PageInit-zeroed page, so its pads are deterministically 0;
    // a whole-struct write would copy uninit padding into WAL'd bytes.
    pub fn page_image(&self) -> [u8; 48] {
        let mut b = [0u8; 48];
        b[0..4].copy_from_slice(&self.btm_magic.to_ne_bytes());
        b[4..8].copy_from_slice(&self.btm_version.to_ne_bytes());
        b[8..12].copy_from_slice(&self.btm_root.to_ne_bytes());
        b[12..16].copy_from_slice(&self.btm_level.to_ne_bytes());
        b[16..20].copy_from_slice(&self.btm_fastroot.to_ne_bytes());
        b[20..24].copy_from_slice(&self.btm_fastlevel.to_ne_bytes());
        b[24..28].copy_from_slice(&self.btm_last_cleanup_num_delpages.to_ne_bytes());
        b[32..40].copy_from_slice(&self.btm_last_cleanup_num_heap_tuples.to_ne_bytes());
        b[40] = self.btm_allequalimage as u8;
        b
    }
}

const _: () = assert!(core::mem::size_of::<BTMetaPageData>() == 48);
const _: () = assert!(core::mem::offset_of!(BTMetaPageData, btm_magic) == 0);
const _: () = assert!(core::mem::offset_of!(BTMetaPageData, btm_version) == 4);
const _: () = assert!(core::mem::offset_of!(BTMetaPageData, btm_root) == 8);
const _: () = assert!(core::mem::offset_of!(BTMetaPageData, btm_level) == 12);
const _: () = assert!(core::mem::offset_of!(BTMetaPageData, btm_fastroot) == 16);
const _: () = assert!(core::mem::offset_of!(BTMetaPageData, btm_fastlevel) == 20);
const _: () = assert!(core::mem::offset_of!(BTMetaPageData, btm_last_cleanup_num_delpages) == 24);
const _: () =
    assert!(core::mem::offset_of!(BTMetaPageData, btm_last_cleanup_num_heap_tuples) == 32);
const _: () = assert!(core::mem::offset_of!(BTMetaPageData, btm_allequalimage) == 40);

pub const BTREE_METAPAGE: BlockNumber = 0;
pub const BTREE_MAGIC: u32 = 0x053162;
pub const BTREE_VERSION: u32 = 4;
pub const BTREE_MIN_VERSION: u32 = 2;
pub const BTREE_NOVAC_VERSION: u32 = 3;

const fn maxalign(len: usize) -> usize {
    (len + 7) & !7
}

pub const BTMaxItemSize: Size = maxalign_down(
    (BLCKSZ
        - maxalign(SizeOfPageHeaderData + 3 * core::mem::size_of::<ItemIdData>())
        - maxalign(core::mem::size_of::<BTPageOpaqueData>()))
        / 3,
) - maxalign(core::mem::size_of::<ItemPointerData>());

pub const BTMaxItemSizeNoHeapTid: Size = maxalign_down(
    (BLCKSZ
        - maxalign(SizeOfPageHeaderData + 3 * core::mem::size_of::<ItemIdData>())
        - maxalign(core::mem::size_of::<BTPageOpaqueData>()))
        / 3,
);

const fn maxalign_down(len: usize) -> usize {
    len & !7
}

const _: () = assert!(BTMaxItemSize == 2704);
const _: () = assert!(BTMaxItemSizeNoHeapTid == 2712);

pub const MaxTIDsPerBTreePage: usize = (BLCKSZ
    - SizeOfPageHeaderData
    - core::mem::size_of::<BTPageOpaqueData>())
    / core::mem::size_of::<ItemPointerData>();

const _: () = assert!(MaxTIDsPerBTreePage == 1358);

pub const BTREE_MIN_FILLFACTOR: i32 = 10;
pub const BTREE_DEFAULT_FILLFACTOR: i32 = 90;
pub const BTREE_NONLEAF_FILLFACTOR: i32 = 70;
pub const BTREE_SINGLEVAL_FILLFACTOR: i32 = 96;

pub const P_NONE: BlockNumber = 0;

#[inline]
pub const fn P_LEFTMOST(opaque: &BTPageOpaqueData) -> bool {
    opaque.btpo_prev == P_NONE
}
#[inline]
pub const fn P_RIGHTMOST(opaque: &BTPageOpaqueData) -> bool {
    opaque.btpo_next == P_NONE
}
#[inline]
pub const fn P_ISLEAF(opaque: &BTPageOpaqueData) -> bool {
    (opaque.btpo_flags & BTP_LEAF) != 0
}
#[inline]
pub const fn P_ISROOT(opaque: &BTPageOpaqueData) -> bool {
    (opaque.btpo_flags & BTP_ROOT) != 0
}
#[inline]
pub const fn P_ISDELETED(opaque: &BTPageOpaqueData) -> bool {
    (opaque.btpo_flags & BTP_DELETED) != 0
}
#[inline]
pub const fn P_ISMETA(opaque: &BTPageOpaqueData) -> bool {
    (opaque.btpo_flags & BTP_META) != 0
}
#[inline]
pub const fn P_ISHALFDEAD(opaque: &BTPageOpaqueData) -> bool {
    (opaque.btpo_flags & BTP_HALF_DEAD) != 0
}
#[inline]
pub const fn P_IGNORE(opaque: &BTPageOpaqueData) -> bool {
    (opaque.btpo_flags & (BTP_DELETED | BTP_HALF_DEAD)) != 0
}
#[inline]
pub const fn P_HAS_GARBAGE(opaque: &BTPageOpaqueData) -> bool {
    (opaque.btpo_flags & BTP_HAS_GARBAGE) != 0
}
#[inline]
pub const fn P_INCOMPLETE_SPLIT(opaque: &BTPageOpaqueData) -> bool {
    (opaque.btpo_flags & BTP_INCOMPLETE_SPLIT) != 0
}
#[inline]
pub const fn P_HAS_FULLXID(opaque: &BTPageOpaqueData) -> bool {
    (opaque.btpo_flags & BTP_HAS_FULLXID) != 0
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BTDeletedPageData {
    pub safexid: FullTransactionId,
}

const _: () = assert!(core::mem::size_of::<BTDeletedPageData>() == 8);

pub const P_HIKEY: OffsetNumber = 1;
pub const P_FIRSTKEY: OffsetNumber = 2;

#[inline]
pub const fn P_FIRSTDATAKEY(opaque: &BTPageOpaqueData) -> OffsetNumber {
    if P_RIGHTMOST(opaque) {
        P_HIKEY
    } else {
        P_FIRSTKEY
    }
}

// INDEX_ALT_TID_MASK is itup.h's INDEX_AM_RESERVED_BIT; carried here until the
// indextuple unit lands.
pub const INDEX_ALT_TID_MASK: uint16 = 0x2000;
pub const BT_OFFSET_MASK: uint16 = 0x0FFF;
pub const BT_STATUS_OFFSET_MASK: uint16 = 0xF000;
pub const BT_PIVOT_HEAP_TID_ATTR: uint16 = 0x1000;
pub const BT_IS_POSTING: uint16 = 0x2000;

const _: () = assert!(BT_OFFSET_MASK as i32 >= INDEX_MAX_KEYS);

#[inline]
pub const fn BTCommuteStrategyNumber(strat: StrategyNumber) -> StrategyNumber {
    BTMaxStrategyNumber + 1 - strat
}

pub const BTORDER_PROC: uint16 = 1;
pub const BTSORTSUPPORT_PROC: uint16 = 2;
pub const BTINRANGE_PROC: uint16 = 3;
pub const BTEQUALIMAGE_PROC: uint16 = 4;
pub const BTOPTIONS_PROC: uint16 = 5;
pub const BTSKIPSUPPORT_PROC: uint16 = 6;
pub const BTNProcs: uint16 = 6;

pub const BT_READ: i32 = BUFFER_LOCK_SHARE;
pub const BT_WRITE: i32 = BUFFER_LOCK_EXCLUSIVE;
