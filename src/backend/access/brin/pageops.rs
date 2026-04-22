use crate::backend::catalog::CatalogError;
use crate::backend::storage::page::bufpage::{
    ITEM_ID_SIZE, ItemIdData, ItemIdFlags, OffsetNumber, PageError, max_align, page_add_item,
    page_get_item_id, page_get_max_offset_number, page_header, page_init,
};
use crate::backend::storage::smgr::BLCKSZ;
use crate::include::access::brin_page::{
    BRIN_EVACUATE_PAGE, BRIN_MAX_ITEM_SIZE, BRIN_META_MAGIC, BRIN_PAGE_CONTENT_OFFSET,
    BRIN_PAGETYPE_META, BrinMetaPageData, brin_page_flags,
    brin_is_meta_page, brin_is_regular_page, set_brin_page_flags, set_brin_page_type,
};

fn write_page_lower(page: &mut [u8; BLCKSZ], lower: u16) {
    page[12..14].copy_from_slice(&lower.to_le_bytes());
}

fn write_page_upper(page: &mut [u8; BLCKSZ], upper: u16) {
    page[14..16].copy_from_slice(&upper.to_le_bytes());
}

pub(crate) fn brin_metapage_data(page: &[u8; BLCKSZ]) -> Result<BrinMetaPageData, CatalogError> {
    if !brin_is_meta_page(page).map_err(page_error)? {
        return Err(CatalogError::Corrupt("BRIN metapage has unexpected page type"));
    }
    let bytes = page
        .get(BRIN_PAGE_CONTENT_OFFSET..BRIN_PAGE_CONTENT_OFFSET + BrinMetaPageData::SIZE)
        .ok_or(CatalogError::Corrupt("truncated BRIN metapage"))?;
    Ok(BrinMetaPageData {
        brin_magic: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
        brin_version: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
        pages_per_range: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
        last_revmap_page: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
    })
}

pub(crate) fn brin_set_metapage_last_revmap_page(
    page: &mut [u8; BLCKSZ],
    last_revmap_page: u32,
) -> Result<(), CatalogError> {
    if !brin_is_meta_page(page).map_err(page_error)? {
        return Err(CatalogError::Corrupt("BRIN metapage has unexpected page type"));
    }
    page[BRIN_PAGE_CONTENT_OFFSET + 12..BRIN_PAGE_CONTENT_OFFSET + 16]
        .copy_from_slice(&last_revmap_page.to_le_bytes());
    Ok(())
}

pub(crate) fn brin_page_init(
    page: &mut [u8; BLCKSZ],
    page_type: u16,
) -> Result<(), CatalogError> {
    page_init(page, crate::include::access::brin_page::BRIN_SPECIAL_SIZE);
    set_brin_page_flags(page, 0).map_err(page_error)?;
    set_brin_page_type(page, page_type).map_err(page_error)?;
    Ok(())
}

pub(crate) fn brin_metapage_init(
    page: &mut [u8; BLCKSZ],
    pages_per_range: u32,
    version: u32,
) -> Result<(), CatalogError> {
    brin_page_init(page, BRIN_PAGETYPE_META)?;
    page[BRIN_PAGE_CONTENT_OFFSET..BRIN_PAGE_CONTENT_OFFSET + 4]
        .copy_from_slice(&BRIN_META_MAGIC.to_le_bytes());
    page[BRIN_PAGE_CONTENT_OFFSET + 4..BRIN_PAGE_CONTENT_OFFSET + 8]
        .copy_from_slice(&version.to_le_bytes());
    page[BRIN_PAGE_CONTENT_OFFSET + 8..BRIN_PAGE_CONTENT_OFFSET + 12]
        .copy_from_slice(&pages_per_range.to_le_bytes());
    page[BRIN_PAGE_CONTENT_OFFSET + 12..BRIN_PAGE_CONTENT_OFFSET + 16]
        .copy_from_slice(&0u32.to_le_bytes());
    write_page_lower(
        page,
        (BRIN_PAGE_CONTENT_OFFSET + BrinMetaPageData::SIZE) as u16,
    );
    Ok(())
}

pub(crate) fn brin_page_get_exact_freespace(page: &[u8; BLCKSZ]) -> Result<usize, CatalogError> {
    Ok(page_header(page).map_err(page_error)?.free_space())
}

pub(crate) fn brin_page_get_freespace(page: &[u8; BLCKSZ]) -> Result<usize, CatalogError> {
    if !brin_is_regular_page(page).map_err(page_error)?
        || (brin_page_flags(page).map_err(page_error)? & BRIN_EVACUATE_PAGE) != 0
    {
        return Ok(0);
    }
    Ok(brin_page_get_exact_freespace(page)?.saturating_sub(ITEM_ID_SIZE))
}

pub(crate) fn brin_can_do_samepage_update(
    page: &[u8; BLCKSZ],
    original_size: usize,
    new_size: usize,
) -> Result<bool, CatalogError> {
    let original_size = max_align(original_size);
    let new_size = max_align(new_size);
    Ok(new_size <= original_size
        || brin_page_get_exact_freespace(page)? >= new_size - original_size)
}

pub(crate) fn brin_regular_page_add_item(
    page: &mut [u8; BLCKSZ],
    item: &[u8],
) -> Result<OffsetNumber, CatalogError> {
    if item.len() > BRIN_MAX_ITEM_SIZE {
        return Err(CatalogError::Io(format!(
            "BRIN tuple size {} exceeds maximum {}",
            item.len(),
            BRIN_MAX_ITEM_SIZE
        )));
    }
    page_add_item(page, item).map_err(page_error)
}

pub(crate) fn page_index_tuple_delete_no_compact(
    page: &mut [u8; BLCKSZ],
    offnum: OffsetNumber,
) -> Result<(), CatalogError> {
    let header = page_header(page).map_err(page_error)?;
    let max_offset = page_get_max_offset_number(page).map_err(page_error)?;
    if offnum == 0 || offnum > max_offset {
        return Err(CatalogError::Corrupt("invalid BRIN tuple offset"));
    }

    let mut item_id = page_get_item_id(page, offnum).map_err(page_error)?;
    if !item_id.has_storage() {
        return Err(CatalogError::Corrupt("BRIN page line pointer has no storage"));
    }

    let aligned_size = max_align(item_id.lp_len as usize);
    let offset = usize::from(item_id.lp_off);
    if offset < usize::from(header.pd_upper)
        || offset + usize::from(item_id.lp_len) > usize::from(header.pd_special)
        || offset != max_align(offset)
    {
        return Err(CatalogError::Corrupt("corrupted BRIN line pointer"));
    }

    if offnum < max_offset {
        item_id = ItemIdData::unused();
        let idx =
            max_align(crate::backend::storage::page::bufpage::SIZE_OF_PAGE_HEADER_DATA)
                + (usize::from(offnum) - 1) * ITEM_ID_SIZE;
        page[idx..idx + ITEM_ID_SIZE].copy_from_slice(&item_id.encode());
    } else {
        let idx =
            max_align(crate::backend::storage::page::bufpage::SIZE_OF_PAGE_HEADER_DATA)
                + (usize::from(offnum) - 1) * ITEM_ID_SIZE;
        page[idx..idx + ITEM_ID_SIZE].fill(0);
        write_page_lower(page, header.pd_lower.saturating_sub(ITEM_ID_SIZE as u16));
    }

    let upper = usize::from(header.pd_upper);
    if offset > upper {
        page.copy_within(upper..offset, upper + aligned_size);
    }
    write_page_upper(page, header.pd_upper + aligned_size as u16);

    let remaining_max = if offnum < max_offset {
        max_offset
    } else {
        max_offset - 1
    };
    for index in 1..=remaining_max {
        let mut existing = page_get_item_id(page, index).map_err(page_error)?;
        if existing.has_storage() && usize::from(existing.lp_off) <= offset {
            existing.lp_off += aligned_size as u16;
            let idx =
                max_align(crate::backend::storage::page::bufpage::SIZE_OF_PAGE_HEADER_DATA)
                    + (usize::from(index) - 1) * ITEM_ID_SIZE;
            page[idx..idx + ITEM_ID_SIZE].copy_from_slice(&existing.encode());
        }
    }

    Ok(())
}

pub(crate) fn page_index_tuple_overwrite(
    page: &mut [u8; BLCKSZ],
    offnum: OffsetNumber,
    new_tuple: &[u8],
) -> Result<bool, CatalogError> {
    let header = page_header(page).map_err(page_error)?;
    let max_offset = page_get_max_offset_number(page).map_err(page_error)?;
    if offnum == 0 || offnum > max_offset {
        return Err(CatalogError::Corrupt("invalid BRIN tuple offset"));
    }

    let mut item_id = page_get_item_id(page, offnum).map_err(page_error)?;
    if !item_id.has_storage() {
        return Err(CatalogError::Corrupt("BRIN page line pointer has no storage"));
    }

    let offset = usize::from(item_id.lp_off);
    let old_size = max_align(item_id.lp_len as usize);
    let new_size = max_align(new_tuple.len());
    if offset < usize::from(header.pd_upper)
        || offset + usize::from(item_id.lp_len) > usize::from(header.pd_special)
        || offset != max_align(offset)
    {
        return Err(CatalogError::Corrupt("corrupted BRIN line pointer"));
    }

    if new_size > old_size + brin_page_get_exact_freespace(page)? {
        return Ok(false);
    }

    let size_diff = old_size as isize - new_size as isize;
    if size_diff != 0 {
        let upper = usize::from(header.pd_upper);
        let target = upper.checked_add_signed(size_diff).ok_or_else(|| {
            CatalogError::Corrupt("BRIN overwrite moved page upper out of range")
        })?;
        page.copy_within(upper..offset, target);
        write_page_upper(
            page,
            header
                .pd_upper
                .checked_add_signed(size_diff as i16)
                .ok_or(CatalogError::Corrupt("invalid BRIN page upper adjustment"))?,
        );

        for index in 1..=max_offset {
            let mut existing = page_get_item_id(page, index).map_err(page_error)?;
            if existing.has_storage() && usize::from(existing.lp_off) <= offset {
                existing.lp_off = existing
                    .lp_off
                    .checked_add_signed(size_diff as i16)
                    .ok_or(CatalogError::Corrupt("invalid BRIN tuple offset adjustment"))?;
                let idx =
                    max_align(crate::backend::storage::page::bufpage::SIZE_OF_PAGE_HEADER_DATA)
                        + (usize::from(index) - 1) * ITEM_ID_SIZE;
                page[idx..idx + ITEM_ID_SIZE].copy_from_slice(&existing.encode());
            }
        }
    }

    let new_offset = (offset as u16)
        .checked_add_signed(size_diff as i16)
        .ok_or(CatalogError::Corrupt("invalid BRIN overwrite tuple offset"))?;
    item_id.lp_off = new_offset;
    item_id.lp_len = new_tuple.len() as u16;
    let idx =
        max_align(crate::backend::storage::page::bufpage::SIZE_OF_PAGE_HEADER_DATA)
            + (usize::from(offnum) - 1) * ITEM_ID_SIZE;
    page[idx..idx + ITEM_ID_SIZE].copy_from_slice(&item_id.encode());

    let start = usize::from(item_id.lp_off);
    page[start..start + new_tuple.len()].copy_from_slice(new_tuple);
    Ok(true)
}

pub(crate) fn brin_page_start_evacuating(
    page: &mut [u8; BLCKSZ],
) -> Result<bool, CatalogError> {
    if let Err(PageError::NotInitialized) = page_header(page) {
        return Ok(false);
    }
    let max_offset = page_get_max_offset_number(page).map_err(page_error)?;
    for offnum in 1..=max_offset {
        let item_id = page_get_item_id(page, offnum).map_err(page_error)?;
        if item_id.has_storage() && item_id.lp_flags != ItemIdFlags::Unused {
            let flags = brin_page_flags(page).map_err(page_error)? | BRIN_EVACUATE_PAGE;
            set_brin_page_flags(page, flags).map_err(page_error)?;
            return Ok(true);
        }
    }
    Ok(false)
}

fn page_error(err: PageError) -> CatalogError {
    CatalogError::Io(format!("BRIN page error: {err:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::storage::page::bufpage::{
        page_get_item, page_get_item_id, page_get_max_offset_number, page_header,
    };
    use crate::include::access::brin_page::{
        BRIN_CURRENT_VERSION, BRIN_PAGETYPE_REVMAP, BRIN_PAGETYPE_REGULAR, brin_page_type,
        revmap_entry_offset,
    };

    #[test]
    fn metapage_init_matches_postgres_bytes() {
        let mut page = [0u8; BLCKSZ];
        brin_metapage_init(&mut page, 128, BRIN_CURRENT_VERSION).unwrap();

        let header = page_header(&page).unwrap();
        assert_eq!(usize::from(header.pd_lower), BRIN_PAGE_CONTENT_OFFSET + 16);
        assert_eq!(
            &page[BRIN_PAGE_CONTENT_OFFSET..BRIN_PAGE_CONTENT_OFFSET + 16],
            &[
                0xFA, 0x9C, 0x10, 0xA8, // magic
                0x01, 0x00, 0x00, 0x00, // version
                0x80, 0x00, 0x00, 0x00, // pages_per_range
                0x00, 0x00, 0x00, 0x00, // last_revmap_page
            ]
        );
        assert_eq!(brin_page_flags(&page).unwrap(), 0);
        assert_eq!(brin_page_type(&page).unwrap(), BRIN_PAGETYPE_META);
        assert_eq!(&page[BLCKSZ - 4..BLCKSZ], &[0, 0, 0x91, 0xF0]);
    }

    #[test]
    fn revmap_page_uses_postgres_trailer_layout() {
        let mut page = [0u8; BLCKSZ];
        brin_page_init(&mut page, BRIN_PAGETYPE_REVMAP).unwrap();
        assert_eq!(brin_page_flags(&page).unwrap(), 0);
        assert_eq!(brin_page_type(&page).unwrap(), BRIN_PAGETYPE_REVMAP);
        assert_eq!(page_get_max_offset_number(&page).unwrap(), 0);
        assert_eq!(revmap_entry_offset(0), BRIN_PAGE_CONTENT_OFFSET);
    }

    #[test]
    fn delete_no_compact_keeps_line_pointer_slot() {
        let mut page = [0u8; BLCKSZ];
        brin_page_init(&mut page, BRIN_PAGETYPE_REGULAR).unwrap();
        let first = brin_regular_page_add_item(&mut page, b"aaa").unwrap();
        let second = brin_regular_page_add_item(&mut page, b"bbbb").unwrap();

        page_index_tuple_delete_no_compact(&mut page, first).unwrap();

        assert_eq!(page_get_max_offset_number(&page).unwrap(), second);
        assert_eq!(page_get_item_id(&page, first).unwrap().lp_flags, ItemIdFlags::Unused);
        assert_eq!(page_get_item(&page, second).unwrap(), b"bbbb");
    }

    #[test]
    fn overwrite_moves_item_without_reassigning_offset() {
        let mut page = [0u8; BLCKSZ];
        brin_page_init(&mut page, BRIN_PAGETYPE_REGULAR).unwrap();
        let first = brin_regular_page_add_item(&mut page, b"aaa").unwrap();
        let second = brin_regular_page_add_item(&mut page, b"bbbb").unwrap();

        assert!(page_index_tuple_overwrite(&mut page, second, b"zzzzzz").unwrap());

        assert_eq!(page_get_max_offset_number(&page).unwrap(), second);
        assert_eq!(page_get_item(&page, first).unwrap(), b"aaa");
        assert_eq!(page_get_item(&page, second).unwrap(), b"zzzzzz");
    }
}
