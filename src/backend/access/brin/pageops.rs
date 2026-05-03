use crate::backend::access::index::buildkeys::map_access_error;
use crate::backend::catalog::CatalogError;
use crate::backend::storage::page::bufpage::OffsetNumber;
use crate::backend::storage::smgr::BLCKSZ;
use crate::include::access::brin_page::BrinMetaPageData;

pub(crate) fn brin_metapage_data(page: &[u8; BLCKSZ]) -> Result<BrinMetaPageData, CatalogError> {
    pgrust_access::brin::pageops::brin_metapage_data(page).map_err(map_access_error)
}

pub(crate) fn brin_set_metapage_last_revmap_page(
    page: &mut [u8; BLCKSZ],
    last_revmap_page: u32,
) -> Result<(), CatalogError> {
    pgrust_access::brin::pageops::brin_set_metapage_last_revmap_page(page, last_revmap_page)
        .map_err(map_access_error)
}

pub(crate) fn brin_page_init(page: &mut [u8; BLCKSZ], page_type: u16) -> Result<(), CatalogError> {
    pgrust_access::brin::pageops::brin_page_init(page, page_type).map_err(map_access_error)
}

pub(crate) fn brin_metapage_init(
    page: &mut [u8; BLCKSZ],
    pages_per_range: u32,
    version: u32,
) -> Result<(), CatalogError> {
    pgrust_access::brin::pageops::brin_metapage_init(page, pages_per_range, version)
        .map_err(map_access_error)
}

pub(crate) fn brin_page_get_exact_freespace(page: &[u8; BLCKSZ]) -> Result<usize, CatalogError> {
    pgrust_access::brin::pageops::brin_page_get_exact_freespace(page).map_err(map_access_error)
}

pub(crate) fn brin_page_get_freespace(page: &[u8; BLCKSZ]) -> Result<usize, CatalogError> {
    pgrust_access::brin::pageops::brin_page_get_freespace(page).map_err(map_access_error)
}

pub(crate) fn brin_can_do_samepage_update(
    page: &[u8; BLCKSZ],
    original_size: usize,
    new_size: usize,
) -> Result<bool, CatalogError> {
    pgrust_access::brin::pageops::brin_can_do_samepage_update(page, original_size, new_size)
        .map_err(map_access_error)
}

pub(crate) fn brin_regular_page_add_item(
    page: &mut [u8; BLCKSZ],
    item: &[u8],
) -> Result<OffsetNumber, CatalogError> {
    pgrust_access::brin::pageops::brin_regular_page_add_item(page, item).map_err(map_access_error)
}

pub(crate) fn page_index_tuple_delete_no_compact(
    page: &mut [u8; BLCKSZ],
    offnum: OffsetNumber,
) -> Result<(), CatalogError> {
    pgrust_access::brin::pageops::page_index_tuple_delete_no_compact(page, offnum)
        .map_err(map_access_error)
}

pub(crate) fn page_index_tuple_overwrite(
    page: &mut [u8; BLCKSZ],
    offnum: OffsetNumber,
    new_tuple: &[u8],
) -> Result<bool, CatalogError> {
    pgrust_access::brin::pageops::page_index_tuple_overwrite(page, offnum, new_tuple)
        .map_err(map_access_error)
}

pub(crate) fn brin_page_start_evacuating(page: &mut [u8; BLCKSZ]) -> Result<bool, CatalogError> {
    pgrust_access::brin::pageops::brin_page_start_evacuating(page).map_err(map_access_error)
}
