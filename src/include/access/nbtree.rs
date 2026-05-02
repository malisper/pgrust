use crate::backend::storage::page::bufpage::{
    PageError, max_align, page_add_item, page_header, page_init, page_special, page_special_mut,
};
use crate::backend::storage::smgr::BLCKSZ;
use crate::include::access::itup::IndexTupleData;
pub use pgrust_nodes::access::BtreeOptions;

pub type BTCycleId = u16;

pub const BTP_LEAF: u16 = 1 << 0;
pub const BTP_ROOT: u16 = 1 << 1;
pub const BTP_DELETED: u16 = 1 << 2;
pub const BTP_META: u16 = 1 << 3;
pub const BTP_HALF_DEAD: u16 = 1 << 4;
pub const BTP_SPLIT_END: u16 = 1 << 5;
pub const BTP_HAS_GARBAGE: u16 = 1 << 6;
pub const BTP_INCOMPLETE_SPLIT: u16 = 1 << 7;
pub const BTP_HAS_FULLXID: u16 = 1 << 8;
pub const BTP_HAS_HIKEY: u16 = 1 << 9;

pub const BTREE_METAPAGE: u32 = 0;
pub const BTREE_MAGIC: u32 = 0x053162;
pub const BTREE_VERSION: u32 = 4;
pub const BTREE_MIN_VERSION: u32 = 2;
pub const BTREE_NOVAC_VERSION: u32 = 3;
pub const BTREE_DEFAULT_FILLFACTOR: u16 = 90;
pub const BTREE_DEFAULT_DEDUPLICATE_ITEMS: bool = true;
pub const BTREE_NONLEAF_FILLFACTOR: u16 = 70;
pub const BTREE_SINGLEVAL_FILLFACTOR: u16 = 96;
pub const P_NONE: u32 = 0;
pub const P_HIKEY: u16 = 1;
pub const P_FIRSTKEY: u16 = 1;
pub const P_FIRSTDATAKEY: u16 = 2;

pub const BT_LESS_STRATEGY_NUMBER: u16 = 1;
pub const BT_LESS_EQUAL_STRATEGY_NUMBER: u16 = 2;
pub const BT_EQUAL_STRATEGY_NUMBER: u16 = 3;
pub const BT_GREATER_EQUAL_STRATEGY_NUMBER: u16 = 4;
pub const BT_GREATER_STRATEGY_NUMBER: u16 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BtPageType {
    Meta,
    Leaf,
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct BTPageOpaqueData {
    pub btpo_prev: u32,
    pub btpo_next: u32,
    pub btpo_level: u32,
    pub btpo_flags: u16,
    pub btpo_cycleid: BTCycleId,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct BTMetaPageData {
    pub btm_magic: u32,
    pub btm_version: u32,
    pub btm_root: u32,
    pub btm_level: u32,
    pub btm_fastroot: u32,
    pub btm_fastlevel: u32,
    pub btm_last_cleanup_num_delpages: u32,
    pub btm_last_cleanup_num_heap_tuples: f64,
    pub btm_allequalimage: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct BTDeletedPageData {
    pub safexid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BtPageError {
    Page(PageError),
    Corrupt(&'static str),
    Tuple(crate::include::access::itup::IndexTupleError),
}

impl From<PageError> for BtPageError {
    fn from(value: PageError) -> Self {
        Self::Page(value)
    }
}

impl From<crate::include::access::itup::IndexTupleError> for BtPageError {
    fn from(value: crate::include::access::itup::IndexTupleError) -> Self {
        Self::Tuple(value)
    }
}

impl BTPageOpaqueData {
    pub const SIZE: usize = 16;

    pub fn new(prev: u32, next: u32, level: u32, flags: u16, cycleid: BTCycleId) -> Self {
        Self {
            btpo_prev: prev,
            btpo_next: next,
            btpo_level: level,
            btpo_flags: flags,
            btpo_cycleid: cycleid,
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.btpo_flags & BTP_LEAF != 0
    }

    pub fn is_meta(&self) -> bool {
        self.btpo_flags & BTP_META != 0
    }

    pub fn is_root(&self) -> bool {
        self.btpo_flags & BTP_ROOT != 0
    }

    pub fn is_leftmost(&self) -> bool {
        self.btpo_prev == P_NONE
    }

    pub fn is_rightmost(&self) -> bool {
        self.btpo_next == P_NONE
    }

    pub fn encode(&self) -> [u8; Self::SIZE] {
        let mut out = [0u8; Self::SIZE];
        out[0..4].copy_from_slice(&self.btpo_prev.to_le_bytes());
        out[4..8].copy_from_slice(&self.btpo_next.to_le_bytes());
        out[8..12].copy_from_slice(&self.btpo_level.to_le_bytes());
        out[12..14].copy_from_slice(&self.btpo_flags.to_le_bytes());
        out[14..16].copy_from_slice(&self.btpo_cycleid.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BtPageError> {
        if bytes.len() < Self::SIZE {
            return Err(BtPageError::Corrupt("bt opaque special space too small"));
        }
        Ok(Self {
            btpo_prev: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            btpo_next: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            btpo_level: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            btpo_flags: u16::from_le_bytes(bytes[12..14].try_into().unwrap()),
            btpo_cycleid: u16::from_le_bytes(bytes[14..16].try_into().unwrap()),
        })
    }
}

impl BTMetaPageData {
    pub const SIZE: usize = 48;

    pub fn new(root: u32, level: u32, allequalimage: bool) -> Self {
        Self {
            btm_magic: BTREE_MAGIC,
            btm_version: BTREE_VERSION,
            btm_root: root,
            btm_level: level,
            btm_fastroot: root,
            btm_fastlevel: level,
            btm_last_cleanup_num_delpages: 0,
            btm_last_cleanup_num_heap_tuples: -1.0,
            btm_allequalimage: allequalimage,
        }
    }

    pub fn encode(&self) -> [u8; Self::SIZE] {
        let mut out = [0u8; Self::SIZE];
        out[0..4].copy_from_slice(&self.btm_magic.to_le_bytes());
        out[4..8].copy_from_slice(&self.btm_version.to_le_bytes());
        out[8..12].copy_from_slice(&self.btm_root.to_le_bytes());
        out[12..16].copy_from_slice(&self.btm_level.to_le_bytes());
        out[16..20].copy_from_slice(&self.btm_fastroot.to_le_bytes());
        out[20..24].copy_from_slice(&self.btm_fastlevel.to_le_bytes());
        out[24..28].copy_from_slice(&self.btm_last_cleanup_num_delpages.to_le_bytes());
        out[32..40].copy_from_slice(&self.btm_last_cleanup_num_heap_tuples.to_le_bytes());
        out[40] = u8::from(self.btm_allequalimage);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BtPageError> {
        if bytes.len() < Self::SIZE {
            return Err(BtPageError::Corrupt("bt metapage contents too small"));
        }
        Ok(Self {
            btm_magic: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            btm_version: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            btm_root: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            btm_level: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            btm_fastroot: u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
            btm_fastlevel: u32::from_le_bytes(bytes[20..24].try_into().unwrap()),
            btm_last_cleanup_num_delpages: u32::from_le_bytes(bytes[24..28].try_into().unwrap()),
            btm_last_cleanup_num_heap_tuples: f64::from_le_bytes(bytes[32..40].try_into().unwrap()),
            btm_allequalimage: bytes[40] != 0,
        })
    }
}

impl BTDeletedPageData {
    pub const SIZE: usize = 4;

    pub fn encode(&self) -> [u8; Self::SIZE] {
        self.safexid.to_le_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BtPageError> {
        if bytes.len() < Self::SIZE {
            return Err(BtPageError::Corrupt("bt deleted page payload too small"));
        }
        Ok(Self {
            safexid: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
        })
    }
}

pub fn bt_max_item_size() -> usize {
    let page_overhead = max_align(
        crate::backend::storage::page::bufpage::SIZE_OF_PAGE_HEADER_DATA
            + 3 * crate::backend::storage::page::bufpage::ITEM_ID_SIZE,
    ) + max_align(BTPageOpaqueData::SIZE);
    let third = (BLCKSZ - page_overhead) / 3;
    max_align(third)
        - max_align(std::mem::size_of::<
            crate::include::access::itemptr::ItemPointerData,
        >())
}

pub fn bt_page_type(page: &[u8; BLCKSZ]) -> Result<BtPageType, BtPageError> {
    let opaque = bt_page_get_opaque(page)?;
    if opaque.is_meta() {
        Ok(BtPageType::Meta)
    } else if opaque.is_leaf() {
        Ok(BtPageType::Leaf)
    } else {
        Ok(BtPageType::Internal)
    }
}

pub fn bt_page_get_opaque(page: &[u8; BLCKSZ]) -> Result<BTPageOpaqueData, BtPageError> {
    BTPageOpaqueData::decode(page_special(page)?)
}

pub fn bt_page_set_opaque(
    page: &mut [u8; BLCKSZ],
    opaque: BTPageOpaqueData,
) -> Result<(), BtPageError> {
    page_special_mut(page)?.copy_from_slice(&opaque.encode());
    Ok(())
}

pub fn bt_page_init(page: &mut [u8; BLCKSZ], flags: u16, level: u32) -> Result<(), BtPageError> {
    page_init(page, BTPageOpaqueData::SIZE);
    bt_page_set_opaque(page, BTPageOpaqueData::new(P_NONE, P_NONE, level, flags, 0))
}

pub fn bt_page_get_meta(page: &[u8; BLCKSZ]) -> Result<BTMetaPageData, BtPageError> {
    BTMetaPageData::decode(
        &page[crate::backend::storage::page::bufpage::max_align(
            crate::backend::storage::page::bufpage::SIZE_OF_PAGE_HEADER_DATA,
        )
            ..crate::backend::storage::page::bufpage::max_align(
                crate::backend::storage::page::bufpage::SIZE_OF_PAGE_HEADER_DATA,
            ) + BTMetaPageData::SIZE],
    )
}

pub fn bt_page_set_meta(page: &mut [u8; BLCKSZ], meta: BTMetaPageData) -> Result<(), BtPageError> {
    let data_start = crate::backend::storage::page::bufpage::max_align(
        crate::backend::storage::page::bufpage::SIZE_OF_PAGE_HEADER_DATA,
    );
    let data_end = data_start + BTMetaPageData::SIZE;
    page[data_start..data_end].copy_from_slice(&meta.encode());
    let mut header = page_header(page)?;
    header.pd_lower = data_end as u16;
    super_write_page_header(page, header);
    Ok(())
}

pub fn bt_init_meta_page(
    page: &mut [u8; BLCKSZ],
    root: u32,
    level: u32,
    allequalimage: bool,
) -> Result<(), BtPageError> {
    bt_page_init(page, BTP_META, 0)?;
    bt_page_set_meta(page, BTMetaPageData::new(root, level, allequalimage))?;
    Ok(())
}

pub fn bt_page_items(page: &[u8; BLCKSZ]) -> Result<Vec<IndexTupleData>, BtPageError> {
    let max_offset = crate::backend::storage::page::bufpage::page_get_max_offset_number(page)?;
    let mut items = Vec::with_capacity(max_offset as usize);
    for offset in 1..=max_offset {
        let bytes = crate::backend::storage::page::bufpage::page_get_item(page, offset)?;
        items.push(IndexTupleData::parse(bytes)?);
    }
    Ok(items)
}

pub fn bt_page_high_key(page: &[u8; BLCKSZ]) -> Result<Option<IndexTupleData>, BtPageError> {
    let opaque = bt_page_get_opaque(page)?;
    if opaque.is_meta() || opaque.btpo_flags & BTP_HAS_HIKEY == 0 {
        return Ok(None);
    }
    Ok(bt_page_items(page)?.into_iter().next())
}

pub fn bt_page_data_items(page: &[u8; BLCKSZ]) -> Result<Vec<IndexTupleData>, BtPageError> {
    let opaque = bt_page_get_opaque(page)?;
    let mut items = bt_page_items(page)?;
    if !opaque.is_meta() && opaque.btpo_flags & BTP_HAS_HIKEY != 0 && !items.is_empty() {
        items.remove(0);
    }
    Ok(items)
}

pub fn bt_page_append_tuple(
    page: &mut [u8; BLCKSZ],
    tuple: &IndexTupleData,
) -> Result<u16, BtPageError> {
    Ok(page_add_item(page, &tuple.serialize())?)
}

pub fn bt_page_replace_items(
    page: &mut [u8; BLCKSZ],
    tuples: &[IndexTupleData],
    opaque: BTPageOpaqueData,
) -> Result<(), BtPageError> {
    let header = page_header(page)?;
    let special_size = BLCKSZ - usize::from(header.pd_special);
    page_init(page, special_size);
    bt_page_set_opaque(page, opaque)?;
    for tuple in tuples {
        bt_page_append_tuple(page, tuple)?;
    }
    Ok(())
}

pub fn bt_page_set_high_key(
    page: &mut [u8; BLCKSZ],
    high_key: &IndexTupleData,
    mut items: Vec<IndexTupleData>,
    mut opaque: BTPageOpaqueData,
) -> Result<(), BtPageError> {
    opaque.btpo_flags |= BTP_HAS_HIKEY;
    let mut rebuilt = Vec::with_capacity(items.len() + 1);
    rebuilt.push(high_key.clone());
    rebuilt.append(&mut items);
    bt_page_replace_items(page, &rebuilt, opaque)
}

pub fn bt_page_set_deleted(
    page: &mut [u8; BLCKSZ],
    mut opaque: BTPageOpaqueData,
    safexid: u32,
) -> Result<(), BtPageError> {
    let header = page_header(page)?;
    let special_size = BLCKSZ - usize::from(header.pd_special);
    page_init(page, special_size);
    opaque.btpo_flags |= BTP_DELETED;
    opaque.btpo_flags &= !(BTP_HALF_DEAD | BTP_HAS_GARBAGE | BTP_INCOMPLETE_SPLIT);
    bt_page_set_opaque(page, opaque)?;
    let start = crate::backend::storage::page::bufpage::max_align(
        crate::backend::storage::page::bufpage::SIZE_OF_PAGE_HEADER_DATA,
    );
    let end = start + BTDeletedPageData::SIZE;
    page[start..end].copy_from_slice(&BTDeletedPageData { safexid }.encode());
    let mut header = page_header(page)?;
    header.pd_lower = end as u16;
    super_write_page_header(page, header);
    Ok(())
}

pub fn bt_page_delete_xid(page: &[u8; BLCKSZ]) -> Result<Option<u32>, BtPageError> {
    let opaque = bt_page_get_opaque(page)?;
    if opaque.btpo_flags & BTP_DELETED == 0 {
        return Ok(None);
    }
    let start = crate::backend::storage::page::bufpage::max_align(
        crate::backend::storage::page::bufpage::SIZE_OF_PAGE_HEADER_DATA,
    );
    let end = start + BTDeletedPageData::SIZE;
    Ok(Some(BTDeletedPageData::decode(&page[start..end])?.safexid))
}

pub fn bt_page_is_recyclable(
    page: &[u8; BLCKSZ],
    oldest_active_xid: u32,
) -> Result<bool, BtPageError> {
    let Some(safexid) = bt_page_delete_xid(page)? else {
        return Ok(false);
    };
    Ok(safexid != 0 && (oldest_active_xid == 0 || safexid < oldest_active_xid))
}

fn super_write_page_header(
    page: &mut [u8; BLCKSZ],
    header: crate::backend::storage::page::bufpage::PageHeaderData,
) {
    page[0..8].copy_from_slice(&header.pd_lsn.to_le_bytes());
    page[8..10].copy_from_slice(&header.pd_checksum.to_le_bytes());
    page[10..12].copy_from_slice(&header.pd_flags.to_le_bytes());
    page[12..14].copy_from_slice(&header.pd_lower.to_le_bytes());
    page[14..16].copy_from_slice(&header.pd_upper.to_le_bytes());
    page[16..18].copy_from_slice(&header.pd_special.to_le_bytes());
    page[18..20].copy_from_slice(&header.pd_pagesize_version.to_le_bytes());
    page[20..24].copy_from_slice(&header.pd_prune_xid.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include::access::itemptr::ItemPointerData;

    #[test]
    fn bt_opaque_roundtrips_exact_bytes() {
        let opaque = BTPageOpaqueData::new(1, 9, 2, BTP_LEAF | BTP_ROOT, 17);
        assert_eq!(BTPageOpaqueData::decode(&opaque.encode()).unwrap(), opaque);
    }

    #[test]
    fn bt_metapage_roundtrips_exact_bytes() {
        let meta = BTMetaPageData::new(1, 0, false);
        let parsed = BTMetaPageData::decode(&meta.encode()).unwrap();
        assert_eq!(parsed, meta);
        assert_eq!(parsed.btm_magic, BTREE_MAGIC);
        assert_eq!(parsed.btm_version, BTREE_VERSION);
    }

    #[test]
    fn bt_page_initialization_uses_special_space_and_meta_shape() {
        let mut page = [0u8; BLCKSZ];
        bt_init_meta_page(&mut page, 1, 0, false).unwrap();
        assert_eq!(bt_page_type(&page).unwrap(), BtPageType::Meta);
        let meta = bt_page_get_meta(&page).unwrap();
        assert_eq!(meta.btm_root, 1);
        let opaque = bt_page_get_opaque(&page).unwrap();
        assert!(opaque.is_meta());
    }

    #[test]
    fn bt_page_rebuilds_with_high_key_first() {
        let mut page = [0u8; BLCKSZ];
        bt_page_init(&mut page, BTP_LEAF, 0).unwrap();
        let high = IndexTupleData::new_raw(
            ItemPointerData {
                block_number: 7,
                offset_number: 0,
            },
            false,
            false,
            false,
            vec![0x99],
        );
        let item = IndexTupleData::new_raw(
            ItemPointerData {
                block_number: 8,
                offset_number: 1,
            },
            false,
            false,
            false,
            vec![0x11],
        );
        let opaque = bt_page_get_opaque(&page).unwrap();
        bt_page_set_high_key(&mut page, &high, vec![item.clone()], opaque).unwrap();
        let tuples = bt_page_items(&page).unwrap();
        assert_eq!(tuples[0], high);
        assert_eq!(tuples[1], item);
    }
}
