use crate::backend::storage::smgr::BLCKSZ;
pub use pgrust_core::storage::OffsetNumber;

pub const PG_PAGE_LAYOUT_VERSION: u8 = 4;
pub const ITEM_ID_SIZE: usize = 4;
pub const SIZE_OF_PAGE_HEADER_DATA: usize = 24;
pub const MAXALIGN: usize = 8;
pub const PD_ALL_VISIBLE: u16 = 0x0004;
pub const MAX_HEAP_TUPLE_SIZE: usize =
    BLCKSZ - ((SIZE_OF_PAGE_HEADER_DATA + ITEM_ID_SIZE + (MAXALIGN - 1)) & !(MAXALIGN - 1));

pub type LocationIndex = u16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageError {
    NotInitialized,
    CorruptHeader,
    InvalidOffsetNumber(OffsetNumber),
    InvalidItemId,
    NoSpace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ItemIdFlags {
    Unused = 0,
    Normal = 1,
    Redirect = 2,
    Dead = 3,
}

impl ItemIdFlags {
    fn from_bits(bits: u8) -> Result<Self, PageError> {
        match bits {
            0 => Ok(Self::Unused),
            1 => Ok(Self::Normal),
            2 => Ok(Self::Redirect),
            3 => Ok(Self::Dead),
            _ => Err(PageError::InvalidItemId),
        }
    }

    /// Transmute from a 2-bit value. All 4 values (0-3) are valid variants.
    fn from_bits_unchecked(bits: u8) -> Self {
        debug_assert!(bits <= 3);
        unsafe { std::mem::transmute(bits) }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ItemIdData {
    pub lp_off: u16,
    pub lp_flags: ItemIdFlags,
    pub lp_len: u16,
}

impl ItemIdData {
    pub fn unused() -> Self {
        Self {
            lp_off: 0,
            lp_flags: ItemIdFlags::Unused,
            lp_len: 0,
        }
    }

    pub fn normal(lp_off: u16, lp_len: u16) -> Self {
        Self {
            lp_off,
            lp_flags: ItemIdFlags::Normal,
            lp_len,
        }
    }

    pub fn has_storage(&self) -> bool {
        self.lp_len != 0
    }

    pub fn encode(self) -> [u8; ITEM_ID_SIZE] {
        let raw = (u32::from(self.lp_off) & 0x7fff)
            | ((u32::from(self.lp_flags as u8) & 0x3) << 15)
            | ((u32::from(self.lp_len) & 0x7fff) << 17);
        raw.to_le_bytes()
    }

    pub fn decode(bytes: [u8; ITEM_ID_SIZE]) -> Result<Self, PageError> {
        let raw = u32::from_le_bytes(bytes);
        Ok(Self {
            lp_off: (raw & 0x7fff) as u16,
            lp_flags: ItemIdFlags::from_bits(((raw >> 15) & 0x3) as u8)?,
            lp_len: ((raw >> 17) & 0x7fff) as u16,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageHeaderData {
    pub pd_lsn: u64,
    pub pd_checksum: u16,
    pub pd_flags: u16,
    pub pd_lower: LocationIndex,
    pub pd_upper: LocationIndex,
    pub pd_special: LocationIndex,
    pub pd_pagesize_version: u16,
    pub pd_prune_xid: u32,
}

impl PageHeaderData {
    pub fn new(page_size: usize, special_size: usize) -> Self {
        let special = page_size
            .checked_sub(special_size)
            .expect("special space must fit inside page");
        let lower = max_align(SIZE_OF_PAGE_HEADER_DATA) as u16;
        Self {
            pd_lsn: 0,
            pd_checksum: 0,
            pd_flags: 0,
            pd_lower: lower,
            pd_upper: special as u16,
            pd_special: special as u16,
            pd_pagesize_version: ((page_size as u16) & 0xff00) | u16::from(PG_PAGE_LAYOUT_VERSION),
            pd_prune_xid: 0,
        }
    }

    pub fn page_size(&self) -> usize {
        (self.pd_pagesize_version & 0xff00) as usize
    }

    pub fn page_layout_version(&self) -> u8 {
        (self.pd_pagesize_version & 0x00ff) as u8
    }

    pub fn free_space(&self) -> usize {
        self.pd_upper.saturating_sub(self.pd_lower) as usize
    }
}

pub fn max_align(len: usize) -> usize {
    (len + (MAXALIGN - 1)) & !(MAXALIGN - 1)
}

pub fn page_get_max_offset_number(page: &[u8; BLCKSZ]) -> Result<OffsetNumber, PageError> {
    let header = page_header(page)?;
    if usize::from(header.pd_lower) < max_align(SIZE_OF_PAGE_HEADER_DATA) {
        return Err(PageError::CorruptHeader);
    }
    Ok(
        ((usize::from(header.pd_lower) - max_align(SIZE_OF_PAGE_HEADER_DATA)) / ITEM_ID_SIZE)
            as u16,
    )
}

pub fn page_header(page: &[u8; BLCKSZ]) -> Result<PageHeaderData, PageError> {
    let pd_upper = u16::from_le_bytes([page[14], page[15]]);
    if pd_upper == 0 {
        return Err(PageError::NotInitialized);
    }
    let header = PageHeaderData {
        pd_lsn: u64::from_le_bytes([
            page[0], page[1], page[2], page[3], page[4], page[5], page[6], page[7],
        ]),
        pd_checksum: u16::from_le_bytes([page[8], page[9]]),
        pd_flags: u16::from_le_bytes([page[10], page[11]]),
        pd_lower: u16::from_le_bytes([page[12], page[13]]),
        pd_upper,
        pd_special: u16::from_le_bytes([page[16], page[17]]),
        pd_pagesize_version: u16::from_le_bytes([page[18], page[19]]),
        pd_prune_xid: u32::from_le_bytes([page[20], page[21], page[22], page[23]]),
    };
    if header.pd_lower > header.pd_upper
        || header.pd_upper > header.pd_special
        || usize::from(header.pd_special) > BLCKSZ
        || header.page_size() != BLCKSZ
        || header.page_layout_version() != PG_PAGE_LAYOUT_VERSION
    {
        return Err(PageError::CorruptHeader);
    }
    Ok(header)
}

pub fn page_is_all_visible(page: &[u8; BLCKSZ]) -> Result<bool, PageError> {
    Ok(page_header(page)?.pd_flags & PD_ALL_VISIBLE != 0)
}

pub fn page_set_all_visible(page: &mut [u8; BLCKSZ]) -> Result<(), PageError> {
    let mut header = page_header(page)?;
    header.pd_flags |= PD_ALL_VISIBLE;
    write_page_header(page, header);
    Ok(())
}

pub fn page_clear_all_visible(page: &mut [u8; BLCKSZ]) -> Result<(), PageError> {
    let mut header = page_header(page)?;
    header.pd_flags &= !PD_ALL_VISIBLE;
    write_page_header(page, header);
    Ok(())
}

pub fn page_init(page: &mut [u8; BLCKSZ], special_size: usize) {
    page.fill(0);
    let header = PageHeaderData::new(BLCKSZ, special_size);
    write_page_header(page, header);
}

pub fn page_get_item(page: &[u8; BLCKSZ], offset: OffsetNumber) -> Result<&[u8], PageError> {
    let item_id = page_get_item_id(page, offset)?;
    if !item_id.has_storage() {
        return Err(PageError::InvalidItemId);
    }
    let start = usize::from(item_id.lp_off);
    let end = start + usize::from(item_id.lp_len);
    if end > BLCKSZ {
        return Err(PageError::CorruptHeader);
    }
    Ok(&page[start..end])
}

pub fn page_get_item_id(
    page: &[u8; BLCKSZ],
    offset: OffsetNumber,
) -> Result<ItemIdData, PageError> {
    let max_offset = page_get_max_offset_number(page)?;
    if offset == 0 || offset > max_offset {
        return Err(PageError::InvalidOffsetNumber(offset));
    }
    let idx = max_align(SIZE_OF_PAGE_HEADER_DATA) + (usize::from(offset) - 1) * ITEM_ID_SIZE;
    ItemIdData::decode([page[idx], page[idx + 1], page[idx + 2], page[idx + 3]])
}

/// Like `page_get_item_id` but skips the `page_get_max_offset_number` bounds
/// check. The caller must guarantee `offset` is valid.
pub fn page_get_item_id_unchecked(page: &[u8; BLCKSZ], offset: OffsetNumber) -> ItemIdData {
    let idx = max_align(SIZE_OF_PAGE_HEADER_DATA) + (usize::from(offset) - 1) * ITEM_ID_SIZE;
    let raw = u32::from_le_bytes([page[idx], page[idx + 1], page[idx + 2], page[idx + 3]]);
    ItemIdData {
        lp_off: (raw & 0x7fff) as u16,
        lp_flags: ItemIdFlags::from_bits_unchecked(((raw >> 15) & 0x3) as u8),
        lp_len: ((raw >> 17) & 0x7fff) as u16,
    }
}

/// Like `page_get_item` but skips the bounds check on the offset number.
/// The caller must guarantee that `offset` is a valid offset on this page
/// (e.g. it came from a prior `page_get_max_offset_number` iteration).
pub fn page_get_item_unchecked(page: &[u8; BLCKSZ], offset: OffsetNumber) -> &[u8] {
    let idx = max_align(SIZE_OF_PAGE_HEADER_DATA) + (usize::from(offset) - 1) * ITEM_ID_SIZE;
    let lp_raw = u32::from_le_bytes([page[idx], page[idx + 1], page[idx + 2], page[idx + 3]]);
    let lp_off = (lp_raw & 0x7FFF) as usize;
    let lp_len = ((lp_raw >> 17) & 0x7FFF) as usize;
    &page[lp_off..lp_off + lp_len]
}

pub fn page_add_item(page: &mut [u8; BLCKSZ], item: &[u8]) -> Result<OffsetNumber, PageError> {
    let offset = page_get_max_offset_number(page)? + 1;
    page_add_item_at(page, item, offset)
}

pub fn page_add_item_at(
    page: &mut [u8; BLCKSZ],
    item: &[u8],
    offset: OffsetNumber,
) -> Result<OffsetNumber, PageError> {
    let mut header = page_header(page)?;
    let max_offset = page_get_max_offset_number(page)?;
    if offset == 0 || offset > max_offset + 1 {
        return Err(PageError::InvalidOffsetNumber(offset));
    }

    let aligned_len = max_align(item.len());
    let required = aligned_len + ITEM_ID_SIZE;
    if header.free_space() < required {
        return Err(PageError::NoSpace);
    }

    let new_upper = usize::from(header.pd_upper) - aligned_len;
    let new_lower = usize::from(header.pd_lower) + ITEM_ID_SIZE;
    let old_lower = header.pd_lower;

    page[new_upper..new_upper + item.len()].copy_from_slice(item);
    for b in &mut page[new_upper + item.len()..new_upper + aligned_len] {
        *b = 0;
    }

    if offset <= max_offset {
        let idx = max_align(SIZE_OF_PAGE_HEADER_DATA) + (usize::from(offset) - 1) * ITEM_ID_SIZE;
        page.copy_within(idx..usize::from(old_lower), idx + ITEM_ID_SIZE);
    }
    write_item_id(
        page,
        offset,
        ItemIdData::normal(new_upper as u16, item.len() as u16),
        old_lower,
    );

    header.pd_upper = new_upper as u16;
    header.pd_lower = new_lower as u16;
    write_page_header(page, header);
    Ok(offset)
}

pub fn page_mark_item_dead(page: &mut [u8; BLCKSZ], offset: OffsetNumber) -> Result<(), PageError> {
    let header = page_header(page)?;
    let mut item_id = page_get_item_id(page, offset)?;
    if !item_id.has_storage() {
        return Err(PageError::InvalidItemId);
    }
    item_id.lp_flags = ItemIdFlags::Dead;
    write_item_id(page, offset, item_id, header.pd_lower);
    Ok(())
}

pub fn page_remove_item(page: &mut [u8; BLCKSZ], offset: OffsetNumber) -> Result<(), PageError> {
    let header = page_header(page)?;
    page_get_item_id(page, offset)?;
    write_item_id(page, offset, ItemIdData::unused(), header.pd_lower);
    page_repair_fragmentation(page)
}

pub fn page_repair_fragmentation(page: &mut [u8; BLCKSZ]) -> Result<(), PageError> {
    let mut header = page_header(page)?;
    let max_offset = page_get_max_offset_number(page)?;
    let base_lower = max_align(SIZE_OF_PAGE_HEADER_DATA);
    let original = *page;

    let mut item_ids = Vec::with_capacity(max_offset as usize);
    let mut payloads = Vec::with_capacity(max_offset as usize);
    let mut highest_used = 0usize;
    for offset in 1..=max_offset {
        let item_id = page_get_item_id(&original, offset)?;
        if item_id.lp_flags != ItemIdFlags::Unused {
            highest_used = offset as usize;
        }
        let payload = if item_id.has_storage() {
            let start = usize::from(item_id.lp_off);
            let end = start.saturating_add(usize::from(item_id.lp_len));
            if end > usize::from(header.pd_special) || end > BLCKSZ {
                return Err(PageError::CorruptHeader);
            }
            Some(original[start..end].to_vec())
        } else {
            None
        };
        item_ids.push(item_id);
        payloads.push(payload);
    }

    page[base_lower..usize::from(header.pd_special)].fill(0);
    let mut new_upper = usize::from(header.pd_special);
    for offset in 1..=max_offset {
        let index = usize::from(offset - 1);
        let mut item_id = item_ids[index];
        if item_id.lp_flags == ItemIdFlags::Unused {
            item_id = ItemIdData::unused();
        } else if let Some(bytes) = payloads[index].as_ref() {
            let aligned_len = max_align(bytes.len());
            new_upper = new_upper.saturating_sub(aligned_len);
            page[new_upper..new_upper + bytes.len()].copy_from_slice(bytes);
            for byte in &mut page[new_upper + bytes.len()..new_upper + aligned_len] {
                *byte = 0;
            }
            item_id.lp_off = new_upper as u16;
            item_id.lp_len = bytes.len() as u16;
        }
        if usize::from(offset) <= highest_used {
            write_item_id(page, offset, item_id, header.pd_lower);
        }
    }

    let old_lower = usize::from(header.pd_lower);
    let new_lower = base_lower + highest_used * ITEM_ID_SIZE;
    if new_lower < old_lower {
        page[new_lower..old_lower].fill(0);
    }
    header.pd_lower = new_lower as u16;
    header.pd_upper = new_upper as u16;
    write_page_header(page, header);
    Ok(())
}

pub fn page_special(page: &[u8; BLCKSZ]) -> Result<&[u8], PageError> {
    let header = page_header(page)?;
    Ok(&page[usize::from(header.pd_special)..BLCKSZ])
}

pub fn page_special_mut(page: &mut [u8; BLCKSZ]) -> Result<&mut [u8], PageError> {
    let header = page_header(page)?;
    Ok(&mut page[usize::from(header.pd_special)..BLCKSZ])
}

fn write_item_id(
    page: &mut [u8; BLCKSZ],
    offset: OffsetNumber,
    item_id: ItemIdData,
    old_lower: u16,
) {
    let idx = if offset == 0 {
        unreachable!()
    } else if usize::from(old_lower) == max_align(SIZE_OF_PAGE_HEADER_DATA) && offset == 1 {
        max_align(SIZE_OF_PAGE_HEADER_DATA)
    } else {
        max_align(SIZE_OF_PAGE_HEADER_DATA) + (usize::from(offset) - 1) * ITEM_ID_SIZE
    };
    page[idx..idx + ITEM_ID_SIZE].copy_from_slice(&item_id.encode());
}

fn write_page_header(page: &mut [u8; BLCKSZ], header: PageHeaderData) {
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

    #[test]
    fn item_id_roundtrip() {
        let item = ItemIdData::normal(8120, 48);
        assert_eq!(ItemIdData::decode(item.encode()).unwrap(), item);
    }

    #[test]
    fn page_init_sets_expected_header() {
        let mut page = [0u8; BLCKSZ];
        page_init(&mut page, 0);
        let header = page_header(&page).unwrap();
        assert_eq!(header.page_size(), BLCKSZ);
        assert_eq!(header.page_layout_version(), PG_PAGE_LAYOUT_VERSION);
        assert_eq!(
            usize::from(header.pd_lower),
            max_align(SIZE_OF_PAGE_HEADER_DATA)
        );
        assert_eq!(usize::from(header.pd_upper), BLCKSZ);
        assert_eq!(usize::from(header.pd_special), BLCKSZ);
        assert_eq!(page_get_max_offset_number(&page).unwrap(), 0);
    }

    #[test]
    fn page_add_item_stores_tuple_from_end_of_page() {
        let mut page = [0u8; BLCKSZ];
        page_init(&mut page, 0);
        let data = vec![0xAB; 17];

        let off = page_add_item(&mut page, &data).unwrap();
        assert_eq!(off, 1);

        let header = page_header(&page).unwrap();
        assert_eq!(
            usize::from(header.pd_lower),
            max_align(SIZE_OF_PAGE_HEADER_DATA) + ITEM_ID_SIZE
        );
        assert_eq!(page_get_item(&page, off).unwrap(), data.as_slice());

        let item_id = page_get_item_id(&page, off).unwrap();
        assert_eq!(item_id.lp_flags, ItemIdFlags::Normal);
        assert_eq!(usize::from(item_id.lp_len), data.len());
        assert_eq!(usize::from(item_id.lp_off), BLCKSZ - max_align(data.len()));
    }

    #[test]
    fn page_remove_item_compacts_remaining_tuples() {
        let mut page = [0u8; BLCKSZ];
        page_init(&mut page, 0);
        let first = page_add_item(&mut page, b"first").unwrap();
        let second = page_add_item(&mut page, b"second").unwrap();
        let third = page_add_item(&mut page, b"third").unwrap();

        page_remove_item(&mut page, second).unwrap();

        assert_eq!(page_get_max_offset_number(&page).unwrap(), third);
        assert_eq!(page_get_item(&page, first).unwrap(), b"first");
        assert_eq!(page_get_item(&page, third).unwrap(), b"third");
        assert!(matches!(
            page_get_item(&page, second),
            Err(PageError::InvalidItemId)
        ));
    }
}
