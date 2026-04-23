use crate::backend::catalog::CatalogError;
use crate::backend::storage::page::bufpage::{
    ItemIdFlags, page_get_item, page_get_item_id, page_get_max_offset_number, page_header,
};
use crate::backend::storage::smgr::BLCKSZ;
use crate::include::access::brin_internal::BrinTupleLocation;
use crate::include::access::brin_page::{
    BRIN_META_MAGIC, BRIN_METAPAGE_BLKNO, BRIN_PAGETYPE_REGULAR, BRIN_PAGETYPE_REVMAP,
    REVMAP_PAGE_MAXITEMS, revmap_entry_offset,
};
use crate::include::access::brin_revmap::{
    heap_blk_to_revmap_blk, heap_blk_to_revmap_index, normalize_range_start,
};

use super::pageops::{
    brin_metapage_data, brin_page_get_freespace, brin_page_init, brin_page_start_evacuating,
    brin_regular_page_add_item, brin_set_metapage_last_revmap_page,
    page_index_tuple_delete_no_compact,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BrinRevmap {
    pub(crate) pages_per_range: u32,
    pub(crate) last_revmap_page: u32,
}

pub(crate) fn brin_revmap_initialize(
    index_pages: &[[u8; BLCKSZ]],
) -> Result<BrinRevmap, CatalogError> {
    let metapage = index_pages
        .get(BRIN_METAPAGE_BLKNO as usize)
        .ok_or(CatalogError::Corrupt("missing BRIN metapage"))?;
    let metadata = brin_metapage_data(metapage)?;
    if metadata.brin_magic != BRIN_META_MAGIC {
        return Err(CatalogError::Corrupt("invalid BRIN metapage magic"));
    }
    if metadata.pages_per_range == 0 {
        return Err(CatalogError::Corrupt(
            "BRIN pages_per_range must be positive",
        ));
    }
    Ok(BrinRevmap {
        pages_per_range: metadata.pages_per_range,
        last_revmap_page: metadata.last_revmap_page,
    })
}

pub(crate) fn brin_revmap_extend(
    index_pages: &mut Vec<[u8; BLCKSZ]>,
    revmap: &mut BrinRevmap,
    heap_blk: u32,
) -> Result<(), CatalogError> {
    let heap_blk = normalize_range_start(revmap.pages_per_range, heap_blk);
    let target_block = physical_revmap_block(revmap.pages_per_range, heap_blk);
    while target_block > revmap.last_revmap_page {
        let map_block = revmap.last_revmap_page + 1;
        ensure_page_slot(index_pages, map_block);

        if page_header(&index_pages[map_block as usize]).is_ok()
            && crate::include::access::brin_page::brin_page_type(&index_pages[map_block as usize])
                .map_err(page_error)?
                != BRIN_PAGETYPE_REGULAR
        {
            return Err(CatalogError::Corrupt(
                "unexpected BRIN page type during revmap extension",
            ));
        }

        if brin_page_start_evacuating(&mut index_pages[map_block as usize])? {
            let moved = evacuate_regular_page(index_pages, map_block)?;
            for (tuple_heap_blk, location) in moved {
                revmap_write_entry(
                    index_pages,
                    revmap.pages_per_range,
                    tuple_heap_blk,
                    location,
                )?;
            }
        }

        brin_page_init(&mut index_pages[map_block as usize], BRIN_PAGETYPE_REVMAP)?;
        revmap.last_revmap_page = map_block;
        let metapage = index_pages
            .get_mut(BRIN_METAPAGE_BLKNO as usize)
            .ok_or(CatalogError::Corrupt("missing BRIN metapage"))?;
        brin_set_metapage_last_revmap_page(metapage, map_block)?;
    }
    Ok(())
}

pub(crate) fn brin_revmap_get_location(
    index_pages: &[[u8; BLCKSZ]],
    revmap: &BrinRevmap,
    heap_blk: u32,
) -> Result<BrinTupleLocation, CatalogError> {
    let heap_blk = normalize_range_start(revmap.pages_per_range, heap_blk);
    let map_block = physical_revmap_block(revmap.pages_per_range, heap_blk);
    if map_block > revmap.last_revmap_page {
        return Ok(BrinTupleLocation::invalid());
    }

    let page = index_pages
        .get(map_block as usize)
        .ok_or(CatalogError::Corrupt("missing BRIN revmap page"))?;
    if crate::include::access::brin_page::brin_page_type(page).map_err(page_error)?
        != BRIN_PAGETYPE_REVMAP
    {
        return Err(CatalogError::Corrupt(
            "BRIN revmap page has wrong page type",
        ));
    }

    let entry = revmap_read_entry(
        page,
        heap_blk_to_revmap_index(revmap.pages_per_range, heap_blk),
    )?;
    Ok(entry)
}

pub(crate) fn brin_revmap_set_location(
    index_pages: &mut Vec<[u8; BLCKSZ]>,
    revmap: &mut BrinRevmap,
    heap_blk: u32,
    location: BrinTupleLocation,
) -> Result<(), CatalogError> {
    let heap_blk = normalize_range_start(revmap.pages_per_range, heap_blk);
    brin_revmap_extend(index_pages, revmap, heap_blk)?;
    revmap_write_entry(index_pages, revmap.pages_per_range, heap_blk, location)
}

pub(crate) fn brin_revmap_desummarize_range(
    index_pages: &mut Vec<[u8; BLCKSZ]>,
    revmap: &mut BrinRevmap,
    heap_blk: u32,
) -> Result<bool, CatalogError> {
    let heap_blk = normalize_range_start(revmap.pages_per_range, heap_blk);
    let location = brin_revmap_get_location(index_pages, revmap, heap_blk)?;
    if !location.is_valid() {
        return Ok(true);
    }

    let page = index_pages
        .get_mut(location.block as usize)
        .ok_or(CatalogError::Corrupt("BRIN tuple block missing"))?;
    if crate::include::access::brin_page::brin_page_type(page).map_err(page_error)?
        != BRIN_PAGETYPE_REGULAR
    {
        return Err(CatalogError::Corrupt(
            "BRIN revmap points to non-regular page",
        ));
    }
    let max_offset = page_get_max_offset_number(page).map_err(page_error)?;
    if location.offset == 0 || location.offset > max_offset {
        return Err(CatalogError::Corrupt("corrupted BRIN revmap tuple offset"));
    }
    let item_id = page_get_item_id(page, location.offset).map_err(page_error)?;
    if item_id.lp_flags == ItemIdFlags::Unused || !item_id.has_storage() {
        return Err(CatalogError::Corrupt("corrupted BRIN revmap tuple pointer"));
    }

    page_index_tuple_delete_no_compact(page, location.offset)?;
    revmap_write_entry(
        index_pages,
        revmap.pages_per_range,
        heap_blk,
        BrinTupleLocation::invalid(),
    )?;
    Ok(true)
}

pub(crate) fn brin_revmap_get_tuple_bytes<'a>(
    index_pages: &'a [[u8; BLCKSZ]],
    revmap: &BrinRevmap,
    heap_blk: u32,
) -> Result<Option<(BrinTupleLocation, &'a [u8])>, CatalogError> {
    let location = brin_revmap_get_location(index_pages, revmap, heap_blk)?;
    if !location.is_valid() {
        return Ok(None);
    }
    let page = index_pages
        .get(location.block as usize)
        .ok_or(CatalogError::Corrupt("BRIN tuple block missing"))?;
    if crate::include::access::brin_page::brin_page_type(page).map_err(page_error)?
        != BRIN_PAGETYPE_REGULAR
    {
        return Err(CatalogError::Corrupt(
            "BRIN revmap points to non-regular page",
        ));
    }
    let bytes = page_get_item(page, location.offset).map_err(page_error)?;
    Ok(Some((location, bytes)))
}

fn physical_revmap_block(pages_per_range: u32, heap_blk: u32) -> u32 {
    heap_blk_to_revmap_blk(pages_per_range, heap_blk) + 1
}

fn ensure_page_slot(index_pages: &mut Vec<[u8; BLCKSZ]>, block: u32) {
    while index_pages.len() <= block as usize {
        index_pages.push([0u8; BLCKSZ]);
    }
}

fn revmap_read_entry(page: &[u8; BLCKSZ], index: usize) -> Result<BrinTupleLocation, CatalogError> {
    if index >= REVMAP_PAGE_MAXITEMS {
        return Err(CatalogError::Corrupt("BRIN revmap index out of range"));
    }
    let offset = revmap_entry_offset(index);
    Ok(BrinTupleLocation {
        block: u32::from_le_bytes(page[offset..offset + 4].try_into().unwrap()),
        offset: u16::from_le_bytes(page[offset + 4..offset + 6].try_into().unwrap()),
    })
}

fn revmap_write_entry(
    index_pages: &mut [[u8; BLCKSZ]],
    pages_per_range: u32,
    heap_blk: u32,
    location: BrinTupleLocation,
) -> Result<(), CatalogError> {
    let map_block = physical_revmap_block(pages_per_range, heap_blk);
    let page = index_pages
        .get_mut(map_block as usize)
        .ok_or(CatalogError::Corrupt("missing BRIN revmap page"))?;
    if crate::include::access::brin_page::brin_page_type(page).map_err(page_error)?
        != BRIN_PAGETYPE_REVMAP
    {
        return Err(CatalogError::Corrupt(
            "BRIN revmap page has wrong page type",
        ));
    }
    let index = heap_blk_to_revmap_index(pages_per_range, heap_blk);
    if index >= REVMAP_PAGE_MAXITEMS {
        return Err(CatalogError::Corrupt("BRIN revmap index out of range"));
    }
    let offset = revmap_entry_offset(index);
    page[offset..offset + 4].copy_from_slice(&location.block.to_le_bytes());
    page[offset + 4..offset + 6].copy_from_slice(&location.offset.to_le_bytes());
    Ok(())
}

fn evacuate_regular_page(
    index_pages: &mut Vec<[u8; BLCKSZ]>,
    source_block: u32,
) -> Result<Vec<(u32, BrinTupleLocation)>, CatalogError> {
    let snapshot = *index_pages
        .get(source_block as usize)
        .ok_or(CatalogError::Corrupt(
            "missing BRIN source page for evacuation",
        ))?;
    let max_offset = page_get_max_offset_number(&snapshot).map_err(page_error)?;
    let mut tuples = Vec::new();
    for offnum in 1..=max_offset {
        let item_id = page_get_item_id(&snapshot, offnum).map_err(page_error)?;
        if item_id.lp_flags == ItemIdFlags::Unused || !item_id.has_storage() {
            continue;
        }
        let bytes = page_get_item(&snapshot, offnum)
            .map_err(page_error)?
            .to_vec();
        if bytes.len() < 4 {
            return Err(CatalogError::Corrupt(
                "truncated BRIN tuple during evacuation",
            ));
        }
        let heap_blk = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        tuples.push((offnum, heap_blk, bytes));
    }

    let mut moved = Vec::with_capacity(tuples.len());
    for (offnum, heap_blk, bytes) in tuples {
        let location = insert_regular_tuple(index_pages, source_block + 1, &bytes)?;
        page_index_tuple_delete_no_compact(
            index_pages
                .get_mut(source_block as usize)
                .ok_or(CatalogError::Corrupt(
                    "missing BRIN source page for deletion",
                ))?,
            offnum,
        )?;
        moved.push((heap_blk, location));
    }
    Ok(moved)
}

fn insert_regular_tuple(
    index_pages: &mut Vec<[u8; BLCKSZ]>,
    minimum_block: u32,
    bytes: &[u8],
) -> Result<BrinTupleLocation, CatalogError> {
    let required_size = crate::backend::storage::page::bufpage::max_align(bytes.len());
    let mut block = minimum_block;
    loop {
        ensure_page_slot(index_pages, block);
        if let Err(crate::backend::storage::page::bufpage::PageError::NotInitialized) =
            page_header(&index_pages[block as usize])
        {
            brin_page_init(&mut index_pages[block as usize], BRIN_PAGETYPE_REGULAR)?;
        }

        if crate::include::access::brin_page::brin_page_type(&index_pages[block as usize])
            .map_err(page_error)?
            != BRIN_PAGETYPE_REGULAR
        {
            block += 1;
            continue;
        }

        if brin_page_get_freespace(&index_pages[block as usize])? < required_size {
            block += 1;
            continue;
        }

        let offset = brin_regular_page_add_item(&mut index_pages[block as usize], bytes)?;
        return Ok(BrinTupleLocation { block, offset });
    }
}

fn page_error(err: crate::backend::storage::page::bufpage::PageError) -> CatalogError {
    CatalogError::Io(format!("BRIN page error: {err:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::access::brin::pageops::{brin_metapage_init, brin_page_init};
    use crate::backend::access::brin::tuple::{brin_build_desc, brin_form_tuple};
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::include::access::brin_internal::BrinMemTuple;
    use crate::include::nodes::primnodes::RelationDesc;

    fn int4_index_desc() -> RelationDesc {
        RelationDesc {
            columns: vec![column_desc("a", SqlType::new(SqlTypeKind::Int4), true)],
        }
    }

    fn sample_summary_bytes(range_start: u32) -> Vec<u8> {
        let desc = brin_build_desc(&int4_index_desc());
        brin_form_tuple(&desc, &BrinMemTuple::new(&desc, range_start))
            .unwrap()
            .bytes
    }

    #[test]
    fn revmap_entry_roundtrip_uses_postgres_offsets() {
        let mut pages = vec![[0u8; BLCKSZ]];
        brin_metapage_init(&mut pages[0], 4, 1).unwrap();
        let mut revmap = brin_revmap_initialize(&pages).unwrap();
        brin_revmap_extend(&mut pages, &mut revmap, 0).unwrap();

        let location = BrinTupleLocation {
            block: 9,
            offset: 3,
        };
        brin_revmap_set_location(&mut pages, &mut revmap, 0, location).unwrap();

        assert_eq!(revmap.last_revmap_page, 1);
        assert_eq!(
            brin_revmap_get_location(&pages, &revmap, 0).unwrap(),
            location
        );
        let raw = &pages[1][revmap_entry_offset(0)..revmap_entry_offset(0) + 6];
        assert_eq!(raw, &[9, 0, 0, 0, 3, 0]);
    }

    #[test]
    fn revmap_extension_evacuates_regular_page() {
        let mut pages = vec![[0u8; BLCKSZ], [0u8; BLCKSZ], [0u8; BLCKSZ]];
        brin_metapage_init(&mut pages[0], 1, 1).unwrap();
        brin_page_init(&mut pages[1], BRIN_PAGETYPE_REVMAP).unwrap();
        brin_page_init(&mut pages[2], BRIN_PAGETYPE_REGULAR).unwrap();
        brin_set_metapage_last_revmap_page(&mut pages[0], 1).unwrap();

        let tuple = sample_summary_bytes(0);
        let tuple_off =
            crate::backend::storage::page::bufpage::page_add_item(&mut pages[2], &tuple).unwrap();

        let mut revmap = brin_revmap_initialize(&pages).unwrap();
        brin_revmap_set_location(
            &mut pages,
            &mut revmap,
            0,
            BrinTupleLocation {
                block: 2,
                offset: tuple_off,
            },
        )
        .unwrap();

        let target_heap_blk = REVMAP_PAGE_MAXITEMS as u32;
        brin_revmap_extend(&mut pages, &mut revmap, target_heap_blk).unwrap();

        assert_eq!(revmap.last_revmap_page, 2);
        assert_eq!(brin_metapage_data(&pages[0]).unwrap().last_revmap_page, 2);
        assert_eq!(
            crate::include::access::brin_page::brin_page_type(&pages[2]).unwrap(),
            BRIN_PAGETYPE_REVMAP
        );
        assert_eq!(
            crate::include::access::brin_page::brin_page_type(&pages[3]).unwrap(),
            BRIN_PAGETYPE_REGULAR
        );

        let new_location = brin_revmap_get_location(&pages, &revmap, 0).unwrap();
        assert_eq!(new_location.block, 3);
        assert_eq!(
            page_get_item(&pages[3], new_location.offset).unwrap(),
            tuple.as_slice()
        );
    }

    #[test]
    fn desummarize_clears_revmap_entry_and_tuple() {
        let mut pages = vec![[0u8; BLCKSZ]];
        brin_metapage_init(&mut pages[0], 4, 1).unwrap();
        let mut revmap = brin_revmap_initialize(&pages).unwrap();
        brin_revmap_extend(&mut pages, &mut revmap, 0).unwrap();
        pages.push([0u8; BLCKSZ]);
        brin_page_init(&mut pages[2], BRIN_PAGETYPE_REGULAR).unwrap();

        let tuple = sample_summary_bytes(0);
        let off =
            crate::backend::storage::page::bufpage::page_add_item(&mut pages[2], &tuple).unwrap();
        brin_revmap_set_location(
            &mut pages,
            &mut revmap,
            0,
            BrinTupleLocation {
                block: 2,
                offset: off,
            },
        )
        .unwrap();

        assert!(brin_revmap_desummarize_range(&mut pages, &mut revmap, 0).unwrap());
        assert_eq!(
            brin_revmap_get_location(&pages, &revmap, 0).unwrap(),
            BrinTupleLocation::invalid()
        );
        assert_eq!(page_get_max_offset_number(&pages[2]).unwrap(), 0);
    }
}
